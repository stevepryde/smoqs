use crate::errors::{MyError, MyResult};
use crate::misc::{
    get_attributes, get_message_attribute_names, get_message_attributes, get_new_id,
};
use crate::state::{Message, SQSQueue, State};
use crate::xml::FormatXML;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub fn list_queues(_form: HashMap<String, String>, state: Arc<RwLock<State>>) -> MyResult<String> {
    let s = state.read()?;
    let queue_urls: Vec<String> = s
        .queues
        .values()
        .map(|q| s.get_queue_url(&q.name))
        .collect();

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

pub fn create_queue(form: HashMap<String, String>, state: Arc<RwLock<State>>) -> MyResult<String> {
    let queue_name = form
        .get("QueueName")
        .ok_or_else(|| MyError::MissingParameter("QueueName".to_string()))?;
    let attributes = get_attributes(&form);
    let q = SQSQueue::new(queue_name, attributes);
    let mut s = state.write()?;
    s.add_queue(q);
    let queue_url = s.get_queue_url(&queue_name);

    let output = format!(
        "<CreateQueueResponse>\
            <CreateQueueResult>\
                <QueueUrl>{}</QueueUrl>\
            </CreateQueueResult>\
            <ResponseMetadata>\
                <RequestId>{}</RequestId>\
            </ResponseMetadata>\
        </CreateQueueResponse>",
        queue_url,
        get_new_id(),
    );
    Ok(output)
}

pub fn delete_queue(form: HashMap<String, String>, state: Arc<RwLock<State>>) -> MyResult<String> {
    let queue_url = form
        .get("QueueUrl")
        .ok_or_else(|| MyError::MissingParameter("QueueUrl".to_string()))?;
    let mut s = state.write()?;
    s.remove_queue(queue_url);

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

pub fn get_queue_attributes(
    form: HashMap<String, String>,
    state: Arc<RwLock<State>>,
) -> MyResult<String> {
    let queue_url = form
        .get("QueueUrl")
        .ok_or_else(|| MyError::MissingParameter("QueueUrl".to_string()))?;
    let s = state.read()?;
    if let Some(q) = s.queues.get(queue_url) {
        let mut attributes_str = String::new();
        for (k, v) in q.attributes.iter() {
            attributes_str.push_str(&format!(
                "<Attribute>\
                    <Name>{}</Name>\
                    <Value>{}</Value>\
                 </Attribute>",
                k, v
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

pub fn set_queue_attributes(
    form: HashMap<String, String>,
    state: Arc<RwLock<State>>,
) -> MyResult<String> {
    let queue_url = form
        .get("QueueUrl")
        .ok_or_else(|| MyError::MissingParameter("QueueUrl".to_string()))?;
    let attributes = get_attributes(&form);
    let mut s = state.write()?;
    if let Some(q) = s.queues.get_mut(queue_url) {
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

pub fn send_message(form: HashMap<String, String>, state: Arc<RwLock<State>>) -> MyResult<String> {
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
    let mut s = state.write()?;
    if let Some(q) = s.queues.get_mut(queue_url) {
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

pub fn receive_message(
    form: HashMap<String, String>,
    state: Arc<RwLock<State>>,
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
    let attribute_names = get_message_attribute_names(&form);
    let mut s = state.write()?;
    if let Some(q) = s.queues.get_mut(queue_url) {
        // Pop messages.
        let messages = q.receive_messages(max_count);
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
    } else {
        Err(MyError::QueueNotFound(queue_url.clone()))
    }
}
