use crate::misc::{escape_xml, get_new_id};
use chrono::{DateTime, Utc};
use log::warn;
use md5::{Digest, Md5};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};

pub struct State {
    pub account_id: String,
    region: String,
    endpoint_url: String,
    pub queues: HashMap<QueuePath, SQSQueue>,
    pub topics: HashMap<TopicArn, SNSTopic>,
    pub received_messages: HashMap<ReceiveHandle, ReceivedMessage>,
}

impl State {
    pub fn new(port: u16, region: &str, account_id: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            endpoint_url: format!("http://localhost:{}", port),
            queues: HashMap::new(),
            topics: HashMap::new(),
            received_messages: HashMap::new(),
        }
    }

    pub fn add_queue(&mut self, queue: SQSQueue) -> bool {
        let url = self.get_queue_url(&queue.name);
        let path = self.get_queue_path(&url);
        match self.queues.entry(path) {
            Entry::Vacant(v) => {
                v.insert(queue);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    pub fn remove_queue(&mut self, queue_url: &str) -> bool {
        let path = self.get_queue_path(queue_url);
        self.queues.remove(&path).is_some()
    }

    pub fn get_queue_path(&self, queue_url: &str) -> QueuePath {
        if queue_url.starts_with("arn") {
            let p = queue_url.rsplit(':').next().unwrap_or(queue_url);
            QueuePath(p.to_string())
        } else {
            let p = queue_url.rsplit('/').next().unwrap_or(queue_url);
            QueuePath(p.to_string())
        }
    }

    pub fn get_queue_url(&self, queue_name: &str) -> String {
        format!("{}/{}/{}", self.endpoint_url, self.account_id, queue_name)
    }

    pub fn add_topic(&mut self, topic: SNSTopic) -> bool {
        let arn = self.get_topic_arn(&topic.name);
        match self.topics.entry(arn) {
            Entry::Vacant(v) => {
                v.insert(topic);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    pub fn remove_topic(&mut self, topic_arn: &TopicArn) -> bool {
        self.topics.remove(topic_arn).is_some()
    }

    pub fn get_topic_arn(&self, topic_name: &str) -> TopicArn {
        TopicArn(format!(
            "arn:aws:sns:{}:{}:{}",
            self.region, self.account_id, topic_name
        ))
    }

    pub fn add_received_message(
        &mut self,
        message: Message,
        queue_path: QueuePath,
        timeout_seconds: u32,
    ) -> ReceiveHandle {
        let handle = ReceiveHandle::new();
        let rec_msg = ReceivedMessage::new(message, queue_path, timeout_seconds);
        self.received_messages.insert(handle.clone(), rec_msg);
        handle
    }

    pub fn delete_received_message(&mut self, handle: &ReceiveHandle) {
        self.received_messages.remove(handle);
    }
}

#[derive(Debug, Clone)]
pub struct Message {
    pub id: String,
    pub content: String,
    attributes: HashMap<String, String>,
    pub receive_count: u8,
    pub receipt_handle: ReceiveHandle,
}

impl Message {
    pub fn new(content: &str, attributes: HashMap<String, String>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            content: content.to_string(),
            attributes,
            receive_count: 0,
            receipt_handle: ReceiveHandle::new(),
        }
    }

    pub fn get_content_md5(&self) -> String {
        let mut hasher = Md5::new();
        hasher.update(self.content.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    pub fn get_attribute_md5(&self) -> String {
        let mut hasher = Md5::new();
        for (k, v) in self.attributes.iter() {
            hasher.update(k.as_bytes());
            hasher.update(v.as_bytes());
        }
        format!("{:x}", hasher.finalize())
    }

    pub fn get_attribute_xml(&self, attribute_names: &[String]) -> String {
        let mut attributes_str = String::new();
        for k in attribute_names {
            if let Some(v) = self.attributes.get(k) {
                attributes_str.push_str(&format!(
                    "<Attribute>\
                        <Name>{}</Name>\
                        <Value>{}</Value>\
                     </Attribute>",
                    escape_xml(k),
                    escape_xml(v)
                ));
            }
        }
        attributes_str
    }

    pub fn get_message_xml(&self, attribute_names: &[String]) -> String {
        format!(
            "<Message>\
              <MessageId>{}</MessageId>\
              <ReceiptHandle>\
                {}\
              </ReceiptHandle>\
              <MD5OfBody>{}</MD5OfBody>\
              <Body>{}</Body>\
              {}\
            </Message>",
            self.id,
            self.receipt_handle.0,
            self.get_content_md5(),
            escape_xml(&self.content),
            self.get_attribute_xml(attribute_names),
        )
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct QueuePath(String);

pub struct SQSQueue {
    pub name: String,
    pub attributes: HashMap<String, String>,
    pub messages: VecDeque<Message>,
    // Ring the bell when sending messages, if one exists.
    // This allows us to wait for messages efficiently without polling.
    pub bell: Option<tokio::sync::oneshot::Sender<bool>>,
}

impl SQSQueue {
    pub fn new(name: &str, attributes: HashMap<String, String>) -> Self {
        Self {
            name: name.to_string(),
            attributes,
            messages: VecDeque::new(),
            bell: None,
        }
    }

    pub fn get_attribute(&self, key: &str, default: &str) -> String {
        self.attributes
            .get(key)
            .cloned()
            .unwrap_or(default.to_string())
    }

    pub fn set_attribute_default(&mut self, key: &str, default: &str) {
        if let Entry::Vacant(v) = self.attributes.entry(key.to_string()) {
            v.insert(default.to_string());
        }
    }

    pub fn has_message(&self) -> bool {
        !self.messages.is_empty()
    }

    pub fn get_waiter(&mut self) -> tokio::sync::oneshot::Receiver<bool> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.bell = Some(tx);
        rx
    }

    pub fn send_message(&mut self, message: Message) {
        self.messages.push_back(message);
        if let Some(sender) = self.bell.take() {
            if let Err(e) = sender.send(true) {
                warn!("Failed to notify receiver of message: {:?}", e);
            }
        }
    }

    pub fn receive_messages(&mut self, count: u8) -> Vec<Message> {
        let mut messages_out = Vec::with_capacity(count as usize);
        for _ in 0..count {
            match self.messages.pop_front() {
                Some(x) => messages_out.push(x),
                None => break,
            }
        }
        messages_out
    }
}

pub struct SNSSubscription {
    pub id: String,
    pub arn: String,
    pub owner: String,
    pub protocol: String,
    pub endpoint: String,
    pub topic_arn: String,
}

impl SNSSubscription {
    pub fn new_sqs(topic_arn: &TopicArn, endpoint: &str, account_id: &str) -> Self {
        let id = get_new_id();
        let arn = format!("{}:{}", topic_arn.0, id);
        Self {
            id,
            arn,
            owner: account_id.to_string(),
            protocol: "sqs".to_string(),
            endpoint: endpoint.to_string(),
            topic_arn: topic_arn.0.clone(),
        }
    }

    pub fn get_subscription_xml(&self) -> String {
        format!(
            "<member>\
                <TopicArn>{}</TopicArn>\
                <Protocol>{}</Protocol>\
                <SubscriptionArn>{}</SubscriptionArn>\
                <Owner>{}</Owner>\
                <Endpoint>{}</Endpoint>\
            </member>",
            escape_xml(&self.topic_arn),
            escape_xml(&self.protocol),
            escape_xml(&self.arn),
            escape_xml(&self.owner),
            escape_xml(&self.endpoint)
        )
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TopicArn(pub String);

pub struct SNSTopic {
    pub name: String,
    pub arn: String,
    pub attributes: HashMap<String, String>,
    pub subscriptions: Vec<SNSSubscription>,
}

impl SNSTopic {
    pub fn new(name: &str, arn: &TopicArn, attributes: HashMap<String, String>) -> Self {
        Self {
            name: name.to_string(),
            arn: arn.0.clone(),
            attributes,
            subscriptions: Vec::new(),
        }
    }

    pub fn add_subscription(&mut self, subscription: SNSSubscription) {
        for sub in self.subscriptions.iter() {
            if sub.topic_arn == subscription.topic_arn && sub.endpoint == subscription.endpoint {
                // Already exists - do nothing.
                return;
            }
        }
        self.subscriptions.push(subscription);
    }

    pub fn remove_subscription(&mut self, subscription_arn: &str) {
        self.subscriptions.retain(|s| s.arn != subscription_arn)
    }

    pub fn get_queue_urls(&self) -> Vec<String> {
        self.subscriptions
            .iter()
            .map(|s| s.endpoint.clone())
            .collect()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ReceiveHandle(pub String);

impl ReceiveHandle {
    pub fn new() -> Self {
        Self(get_new_id())
    }
}

#[derive(Debug, Clone)]
pub struct ReceivedMessage {
    pub message: Message,
    pub queue_path: QueuePath,
    expires: DateTime<Utc>,
}

impl ReceivedMessage {
    pub fn new(message: Message, queue_path: QueuePath, timeout_seconds: u32) -> Self {
        Self {
            message,
            queue_path,
            expires: Utc::now() + chrono::Duration::seconds(timeout_seconds as i64),
        }
    }

    pub fn has_expired(&self) -> bool {
        Utc::now() > self.expires
    }

    pub fn set_visibility_timeout(&mut self, visibility_timeout: u32) {
        self.expires = Utc::now() + chrono::Duration::seconds(visibility_timeout as i64)
    }
}
