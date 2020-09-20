use crate::misc::get_new_id;
use md5::{Digest, Md5};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};

pub struct State {
    pub account_id: String,
    region: String,
    endpoint_url: String,
    pub queues: HashMap<String, SQSQueue>,
    pub topics: HashMap<String, SNSTopic>,
}

impl State {
    pub fn new(port: u16) -> Self {
        Self {
            account_id: "000000000000".to_string(),
            region: "ap-southeast-2".to_string(),
            endpoint_url: format!("http://localhost:{}", port),
            queues: HashMap::new(),
            topics: HashMap::new(),
        }
    }

    pub fn add_queue(&mut self, queue: SQSQueue) -> bool {
        let url = self.get_queue_url(&queue.name);
        match self.queues.entry(url) {
            Entry::Vacant(v) => {
                v.insert(queue);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    pub fn remove_queue(&mut self, queue_url: &str) -> bool {
        self.queues.remove(queue_url).is_some()
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

    pub fn remove_topic(&mut self, topic_arn: &str) -> bool {
        self.topics.remove(topic_arn).is_some()
    }

    pub fn get_topic_arn(&self, topic_name: &str) -> String {
        format!(
            "arn:aws:sns:{}:{}:{}",
            self.region, self.account_id, topic_name
        )
    }
}

#[derive(Debug, Clone)]
pub struct Message {
    pub id: String,
    content: String,
    attributes: HashMap<String, String>,
}

impl Message {
    pub fn new(content: &str, attributes: HashMap<String, String>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            content: content.to_string(),
            attributes,
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

    pub fn get_attribute_xml(&self, attribute_names: &Vec<String>) -> String {
        let mut attributes_str = String::new();
        for k in attribute_names {
            if let Some(v) = self.attributes.get(k) {
                attributes_str.push_str(&format!(
                    "<Attribute>\
                        <Name>{}</Name>\
                        <Value>{}</Value>\
                     </Attribute>",
                    k, v
                ));
            }
        }
        attributes_str
    }

    pub fn get_message_xml(&self, attribute_names: &Vec<String>) -> String {
        format!(
            "<Message>\
              <MessageId>{}</MessageId>\
              <ReceiptHandle>\
                Dummy\
              </ReceiptHandle>\
              <MD5OfBody>{}</MD5OfBody>\
              <Body>{}</Body>\
              {}\
            </Message>",
            self.id,
            self.get_content_md5(),
            self.content,
            self.get_attribute_xml(attribute_names),
        )
    }
}

pub struct SQSQueue {
    pub name: String,
    pub attributes: HashMap<String, String>,
    pub messages: VecDeque<Message>,
}

impl SQSQueue {
    pub fn new(name: &str, attributes: HashMap<String, String>) -> Self {
        Self {
            name: name.to_string(),
            attributes,
            messages: VecDeque::new(),
        }
    }

    pub fn send_message(&mut self, message: Message) {
        self.messages.push_back(message);
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
    pub fn new_sqs(topic_arn: &str, endpoint: &str, account_id: &str) -> Self {
        let id = get_new_id();
        let arn = format!("{}:{}", topic_arn, id);
        Self {
            id,
            arn,
            owner: account_id.to_string(),
            protocol: "sqs".to_string(),
            endpoint: endpoint.to_string(),
            topic_arn: topic_arn.to_string(),
        }
    }
}

pub struct SNSTopic {
    pub name: String,
    pub arn: String,
    pub attributes: HashMap<String, String>,
    pub subscriptions: Vec<SNSSubscription>,
}

impl SNSTopic {
    pub fn new(name: &str, arn: &str, attributes: HashMap<String, String>) -> Self {
        Self {
            name: name.to_string(),
            arn: arn.to_string(),
            attributes,
            subscriptions: Vec::new(),
        }
    }

    pub fn add_subscription(&mut self, subscription: SNSSubscription) {
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
