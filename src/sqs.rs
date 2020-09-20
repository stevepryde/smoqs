use crate::errors::{MyError, MyResult};
use crate::misc::{
    escape_xml, get_attributes, get_message_attribute_names, get_message_attributes, get_new_id,
};
use crate::state::{Message, ReceiveHandle, SQSQueue, State};
use crate::xml::FormatXML;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::oneshot::Receiver;
use tokio::sync::Mutex;
use tokio::time::Duration;

pub async fn list_queues(
    _form: HashMap<String, String>,
    state: Arc<Mutex<State>>,
) -> MyResult<String> {
    let queue_urls: Vec<String> = {
        let s = state.lock().await;
        s.queues
            .values()
            .map(|q| s.get_queue_url(&q.name))
            .collect()
    };

    let output = format!(
        "<ListQueuesResponse>\
            <ListQueuesResult>\
                {}\
            </ListQueuesResult>\
            <ResponseMetadata>\
                <RequestId>{}</RequestId>\
            </ResponseMetadata>\
        </ListQueuesResponse>",
        queue_urls.to_xml_string("QueueUrl"),
        get_new_id()
    );
    Ok(output)
}

pub async fn create_queue(
    form: HashMap<String, String>,
    state: Arc<Mutex<State>>,
) -> MyResult<String> {
    let queue_name = form
        .get("QueueName")
        .ok_or_else(|| MyError::MissingParameter("QueueName".to_string()))?;
    let attributes = get_attributes(&form);
    let mut q = SQSQueue::new(queue_name, attributes);
    q.set_attribute_default("VisibilityTimeout", "30");

    let queue_url = {
        let mut s = state.lock().await;
        s.add_queue(q);
        s.get_queue_url(&queue_name)
    };

    let output = format!(
        "<CreateQueueResponse>\
            <CreateQueueResult>\
                <QueueUrl>{}</QueueUrl>\
            </CreateQueueResult>\
            <ResponseMetadata>\
                <RequestId>{}</RequestId>\
            </ResponseMetadata>\
        </CreateQueueResponse>",
        escape_xml(&queue_url),
        get_new_id(),
    );
    Ok(output)
}

pub async fn delete_queue(
    form: HashMap<String, String>,
    state: Arc<Mutex<State>>,
) -> MyResult<String> {
    let queue_url = form
        .get("QueueUrl")
        .ok_or_else(|| MyError::MissingParameter("QueueUrl".to_string()))?;
    {
        let mut s = state.lock().await;
        s.remove_queue(queue_url);
    }

    let output = format!(
        "<DeleteQueueResponse>\
            <ResponseMetadata>\
                <RequestId>{}</RequestId>\
            </ResponseMetadata>\
        </DeleteQueueResponse>",
        get_new_id(),
    );
    Ok(output)
}

pub async fn get_queue_attributes(
    form: HashMap<String, String>,
    state: Arc<Mutex<State>>,
) -> MyResult<String> {
    let queue_url = form
        .get("QueueUrl")
        .ok_or_else(|| MyError::MissingParameter("QueueUrl".to_string()))?;
    let s = state.lock().await;
    let path = s.get_queue_path(queue_url);
    if let Some(q) = s.queues.get(&path) {
        let mut attributes_str = String::new();
        for (k, v) in q.attributes.iter() {
            attributes_str.push_str(&format!(
                "<Attribute>\
                    <Name>{}</Name>\
                    <Value>{}</Value>\
                 </Attribute>",
                escape_xml(k),
                escape_xml(v)
            ));
        }
        let output = format!(
            "<GetQueueAttributesResponse>\
                <GetQueueAttributesResult>\
                {}\
                </GetQueueAttributesResult>\
                <ResponseMetadata>\
                    <RequestId>{}</RequestId>\
                </ResponseMetadata>\
            </GetQueueAttributesResponse>",
            attributes_str,
            get_new_id(),
        );
        Ok(output)
    } else {
        Err(MyError::QueueNotFound(queue_url.clone()))
    }
}

pub async fn set_queue_attributes(
    form: HashMap<String, String>,
    state: Arc<Mutex<State>>,
) -> MyResult<String> {
    let queue_url = form
        .get("QueueUrl")
        .ok_or_else(|| MyError::MissingParameter("QueueUrl".to_string()))?;
    let attributes = get_attributes(&form);
    let mut s = state.lock().await;
    let path = s.get_queue_path(queue_url);
    if let Some(q) = s.queues.get_mut(&path) {
        q.attributes = attributes;
        let output = format!(
            "<SetQueueAttributesResponse>\
                <ResponseMetadata>\
                    <RequestId>{}</RequestId>\
                </ResponseMetadata>\
            </SetQueueAttributesResponse>",
            get_new_id(),
        );
        Ok(output)
    } else {
        Err(MyError::QueueNotFound(queue_url.clone()))
    }
}

pub async fn send_message(
    form: HashMap<String, String>,
    state: Arc<Mutex<State>>,
) -> MyResult<String> {
    let queue_url = form
        .get("QueueUrl")
        .ok_or_else(|| MyError::MissingParameter("QueueUrl".to_string()))?;
    let message_body = form
        .get("MessageBody")
        .ok_or_else(|| MyError::MissingParameter("MessageBody".to_string()))?;
    // TODO: Support delayed queue.
    let _delay_seconds: u16 = form
        .get("DelaySeconds")
        .map(|sec| sec.parse().ok())
        .flatten()
        .unwrap_or(0);
    let attributes = get_message_attributes(&form);
    let mut s = state.lock().await;
    let path = s.get_queue_path(queue_url);
    if let Some(q) = s.queues.get_mut(&path) {
        let message = Message::new(message_body, attributes);
        let message_id = message.id.clone();
        let md5_message = message.get_content_md5();
        let md5_attributes = message.get_attribute_md5();
        q.send_message(message);

        let output = format!(
            "<SendMessageResponse>\
                <SendMessageResult>\
                    <MD5OfMessageBody>{}</MD5OfMessageBody>\
                    <MD5OfMessageAttributes>{}</MD5OfMessageAttributes>\
                    <MessageId>{}</MessageId>\
                </SendMessageResult>\
                <ResponseMetadata>\
                    <RequestId>{}</RequestId>\
                </ResponseMetadata>\
            </SendMessageResponse>",
            md5_message,
            md5_attributes,
            message_id,
            get_new_id(),
        );
        Ok(output)
    } else {
        Err(MyError::QueueNotFound(queue_url.clone()))
    }
}

enum MessageOrWaiter {
    Message(Vec<Message>),
    Waiter(Receiver<bool>),
}

async fn get_message_or_waiter(
    queue_url: &str,
    max_count: u8,
    state: Arc<Mutex<State>>,
) -> MyResult<MessageOrWaiter> {
    let mut s = state.lock().await;
    let path = s.get_queue_path(queue_url);
    match s.queues.get_mut(&path) {
        Some(q) => {
            match q.has_message() {
                true => {
                    // Pop messages.
                    let messages = q.receive_messages(max_count);
                    Ok(MessageOrWaiter::Message(messages))
                }
                false => Ok(MessageOrWaiter::Waiter(q.get_waiter())),
            }
        }
        None => Err(MyError::QueueNotFound(queue_url.to_string())),
    }
}

pub async fn receive_message(
    form: HashMap<String, String>,
    state: Arc<Mutex<State>>,
) -> MyResult<String> {
    let queue_url = form
        .get("QueueUrl")
        .ok_or_else(|| MyError::MissingParameter("QueueUrl".to_string()))?;
    let mut max_count: u8 = form
        .get("MaxNumberOfMessages")
        .map(|n| n.parse().ok())
        .flatten()
        .unwrap_or(1);
    if max_count > 10 || max_count < 1 {
        max_count = 1;
    }
    let wait_time_seconds: u64 = form
        .get("WaitTimeSeconds")
        .map(|n| n.parse().ok())
        .flatten()
        .unwrap_or(0);
    let visibility_timeout_recv: Option<u32> = form
        .get("VisibilityTimeout")
        .map(|n| n.parse().ok())
        .flatten();
    let attribute_names = get_message_attribute_names(&form);

    let mut messages: Vec<Message> =
        match get_message_or_waiter(&queue_url, max_count, state.clone()).await? {
            MessageOrWaiter::Message(x) => {
                // Message already waiting.
                x
            }
            MessageOrWaiter::Waiter(w) => {
                if wait_time_seconds > 0 {
                    // No messages, but we want to wait.
                    if tokio::time::timeout(Duration::new(wait_time_seconds, 0), w)
                        .await
                        .is_ok()
                    {
                        // We got a message.
                        match get_message_or_waiter(&queue_url, max_count, state.clone()).await? {
                            MessageOrWaiter::Message(x) => x,
                            MessageOrWaiter::Waiter(_) => Vec::new(),
                        }
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                }
            }
        };

    if !messages.is_empty() {
        let mut s = state.lock().await;
        let path = s.get_queue_path(queue_url);
        if let Some(q) = s.queues.get(&path) {
            let visibility_timeout_queue: u32 = q
                .get_attribute("VisibilityTimeout", "600")
                .parse()
                .unwrap_or(600);

            // Prefer visibility timeout of the request, and fallback to that of the queue.
            let visibility_timeout = visibility_timeout_recv.unwrap_or(visibility_timeout_queue);

            // All received messages are cached, so they can be requeued if not
            // deleted within the required timeout.
            for message in messages.iter_mut() {
                message.receive_count += 1;
                message.receipt_handle =
                    s.add_received_message(message.clone(), path.clone(), visibility_timeout);
            }
        }
    }

    let messages_xml: Vec<String> = messages
        .iter()
        .map(|m| m.get_message_xml(&attribute_names))
        .collect();

    let output = format!(
        "<ReceiveMessageResponse>\
          <ReceiveMessageResult>\
            {}\
          </ReceiveMessageResult>\
          <ResponseMetadata>\
            <RequestId>{}</RequestId>\
          </ResponseMetadata>\
        </ReceiveMessageResponse>",
        messages_xml.join(""),
        get_new_id(),
    );
    Ok(output)
}

pub async fn delete_message(
    form: HashMap<String, String>,
    state: Arc<Mutex<State>>,
) -> MyResult<String> {
    let receipt_handle = form
        .get("ReceiptHandle")
        .ok_or_else(|| MyError::MissingParameter("ReceiptHandle".to_string()))?;
    let mut s = state.lock().await;
    s.delete_received_message(&ReceiveHandle(receipt_handle.clone()));

    let output = format!(
        "<DeleteMessageResponse>\
          <ResponseMetadata>\
            <RequestId>{}</RequestId>\
          </ResponseMetadata>\
        </DeleteMessageResponse>",
        get_new_id(),
    );
    Ok(output)
}

pub async fn change_message_visibility(
    form: HashMap<String, String>,
    state: Arc<Mutex<State>>,
) -> MyResult<String> {
    let receipt_handle = form
        .get("ReceiptHandle")
        .ok_or_else(|| MyError::MissingParameter("ReceiptHandle".to_string()))?;
    let visibility_timeout_recv: Option<u32> = form
        .get("VisibilityTimeout")
        .map(|n| n.parse().ok())
        .flatten();

    if let Some(visibility_timeout) = visibility_timeout_recv {
        let mut s = state.lock().await;
        if let Some(msg) = s
            .received_messages
            .get_mut(&ReceiveHandle(receipt_handle.clone()))
        {
            msg.set_visibility_timeout(visibility_timeout);
        }
    }

    let output = format!(
        "<ChangeMessageVisibilityResponse>\
          <ResponseMetadata>\
            <RequestId>{}</RequestId>\
          </ResponseMetadata>\
        </ChangeMessageVisibilityResponse>",
        get_new_id(),
    );
    Ok(output)
}
