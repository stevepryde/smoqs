use crate::errors::{MyError, MyResult};
use crate::misc::{get_attributes, get_message_attributes, get_new_id};
use crate::state::{Message, SNSSubscription, SNSTopic, State, TopicArn};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub fn list_topics(_form: HashMap<String, String>, state: Arc<RwLock<State>>) -> MyResult<String> {
    let s = state.read()?;
    let mut topics_xml = String::new();
    for topic in s.topics.values() {
        let topic_xml = format!("<Topic><TopicArn>{}</TopicArn></Topic>", topic.arn);
        topics_xml.push_str(&topic_xml);
    }

    let output = format!(
        "<ListTopicsResponse>\
            <ListTopicsResult>\
                <Topics>\
                {}\
                </Topics>\
            </ListTopicsResult>\
            <ResponseMetadata>\
                <RequestId>{}</RequestId>\
            </ResponseMetadata>\
        </ListTopicsResponse>",
        topics_xml,
        get_new_id()
    );
    Ok(output)
}

pub fn create_topic(form: HashMap<String, String>, state: Arc<RwLock<State>>) -> MyResult<String> {
    let topic_name = form
        .get("Name")
        .ok_or_else(|| MyError::MissingParameter("Name".to_string()))?;
    let attributes = get_attributes(&form);
    let mut s = state.write()?;
    let arn = s.get_topic_arn(topic_name);
    let topic = SNSTopic::new(topic_name, &arn, attributes);

    s.add_topic(topic);
    let topic_arn = s.get_topic_arn(&topic_name);

    let output = format!(
        "<CreateTopicResponse>\
            <CreateTopicResult>\
                <TopicArn>{}</TopicArn>\
            </CreateTopicResult>\
            <ResponseMetadata>\
                <RequestId>{}</RequestId>\
            </ResponseMetadata>\
        </CreateTopicResponse>",
        topic_arn.0,
        get_new_id(),
    );
    Ok(output)
}

pub fn delete_topic(form: HashMap<String, String>, state: Arc<RwLock<State>>) -> MyResult<String> {
    let topic_arn = form
        .get("TopicArn")
        .ok_or_else(|| MyError::MissingParameter("TopicArn".to_string()))?;
    let mut s = state.write()?;

    s.remove_topic(&TopicArn(topic_arn.clone()));

    let output = format!(
        "<DeleteTopicResponse>\
            <ResponseMetadata>\
                <RequestId>{}</RequestId>\
            </ResponseMetadata>\
        </DeleteTopicResponse>",
        get_new_id(),
    );
    Ok(output)
}

pub fn get_topic_attributes(
    form: HashMap<String, String>,
    state: Arc<RwLock<State>>,
) -> MyResult<String> {
    let topic_arn = form
        .get("TopicArn")
        .ok_or_else(|| MyError::MissingParameter("TopicArn".to_string()))?;
    let s = state.read()?;
    let arn = TopicArn(topic_arn.clone());
    if let Some(t) = s.topics.get(&arn) {
        let mut attributes_str = String::new();
        for (k, v) in t.attributes.iter() {
            attributes_str.push_str(&format!(
                "<Attribute>\
                    <Name>{}</Name>\
                    <Value>{}</Value>\
                 </Attribute>",
                k, v
            ));
        }
        let output = format!(
            "<GetTopicAttributesResponse>\
                <GetTopicAttributesResult>\
                {}\
                </GetTopicAttributesResult>\
                <ResponseMetadata>\
                    <RequestId>{}</RequestId>\
                </ResponseMetadata>\
            </GetTopicAttributesResponse>",
            attributes_str,
            get_new_id(),
        );
        Ok(output)
    } else {
        Err(MyError::TopicNotFound(topic_arn.clone()))
    }
}

pub fn set_topic_attributes(
    form: HashMap<String, String>,
    state: Arc<RwLock<State>>,
) -> MyResult<String> {
    let topic_arn = form
        .get("TopicArn")
        .ok_or_else(|| MyError::MissingParameter("TopicArn".to_string()))?;
    let attributes = get_attributes(&form);
    let mut s = state.write()?;
    let arn = TopicArn(topic_arn.clone());
    if let Some(q) = s.topics.get_mut(&arn) {
        q.attributes = attributes;
        let output = format!(
            "<SetTopicAttributesResponse>\
                <ResponseMetadata>\
                    <RequestId>{}</RequestId>\
                </ResponseMetadata>\
            </SetTopicAttributesResponse>",
            get_new_id(),
        );
        Ok(output)
    } else {
        Err(MyError::TopicNotFound(topic_arn.clone()))
    }
}

pub fn publish(form: HashMap<String, String>, state: Arc<RwLock<State>>) -> MyResult<String> {
    let target_arn = match form.get("TargetArn") {
        Some(x) => x,
        None => form
            .get("TopicArn")
            .ok_or_else(|| MyError::MissingParameter("TopicArn".to_string()))?,
    };

    let message_body = form
        .get("Message")
        .ok_or_else(|| MyError::MissingParameter("Message".to_string()))?;
    let _message_structure = form
        .get("MessageStructure")
        .cloned()
        .unwrap_or_else(|| "json".to_string());

    let attributes = get_message_attributes(&form);
    let mut s = state.write()?;
    let arn = TopicArn(target_arn.clone());
    let queue_urls = match s.topics.get_mut(&arn) {
        Some(t) => t.get_queue_urls(),
        None => {
            return Err(MyError::TopicNotFound(target_arn.clone()));
        }
    };

    // Send message to all subscribed queues.
    let message = Message::new(message_body, attributes);
    let message_id = message.id.clone();

    for queue_url in queue_urls {
        let path = s.get_queue_path(&queue_url);
        if let Some(q) = s.queues.get_mut(&path) {
            q.send_message(message.clone());
        }
    }

    let output = format!(
        "<PublishResponse>\
            <PublishResult>\
                <MessageId>{}</MessageId>\
            </PublishResult>\
            <ResponseMetadata>\
                <RequestId>{}</RequestId>\
            </ResponseMetadata>\
        </PublishResponse>",
        message_id,
        get_new_id(),
    );
    Ok(output)
}

pub fn subscribe(form: HashMap<String, String>, state: Arc<RwLock<State>>) -> MyResult<String> {
    let topic_arn = form
        .get("TopicArn")
        .ok_or_else(|| MyError::MissingParameter("TopicArn".to_string()))?;
    let endpoint = form
        .get("Endpoint")
        .ok_or_else(|| MyError::MissingParameter("TopicArn".to_string()))?;
    // TODO: support other protocols?
    let _protocol = form
        .get("Protocol")
        .ok_or_else(|| MyError::MissingParameter("Protocol".to_string()))?;

    let mut s = state.write()?;
    let account_id = s.account_id.clone();
    let arn = TopicArn(topic_arn.clone());
    if let Some(t) = s.topics.get_mut(&arn) {
        let subscription = SNSSubscription::new_sqs(&arn, endpoint, &account_id);
        let subscription_arn = subscription.arn.clone();
        t.add_subscription(subscription);

        let output = format!(
            "<SubscribeResponse>\
            <SubscribeResult>\
                <SubscriptionArn>{}</SubscriptionArn>\
            </SubscribeResult>\
            <ResponseMetadata>\
                <RequestId>{}</RequestId>\
            </ResponseMetadata>\
        </SubscribeResponse>",
            subscription_arn,
            get_new_id(),
        );
        Ok(output)
    } else {
        Err(MyError::TopicNotFound(topic_arn.clone()))
    }
}

pub fn unsubscribe(form: HashMap<String, String>, state: Arc<RwLock<State>>) -> MyResult<String> {
    let subscription_arn = form
        .get("SubscriptionArn")
        .ok_or_else(|| MyError::MissingParameter("SubscriptionArn".to_string()))?;

    let mut s = state.write()?;
    for topic in s.topics.values_mut() {
        topic.remove_subscription(subscription_arn);
    }

    let output = format!(
        "<UnsubscribeResponse>\
            <ResponseMetadata>\
                <RequestId>{}</RequestId>\
            </ResponseMetadata>\
        </UnsubscribeResponse>",
        get_new_id(),
    );
    Ok(output)
}

pub fn list_subscriptions(
    _form: HashMap<String, String>,
    state: Arc<RwLock<State>>,
) -> MyResult<String> {
    let s = state.read()?;
    let mut subscription_xml = String::new();
    for topic in s.topics.values() {
        for sub in &topic.subscriptions {
            subscription_xml.push_str(&sub.get_subscription_xml());
        }
    }

    let output = format!(
        "<ListSubscriptionsResponse>\
            <ListSubscriptionsResult>\
                <Subscriptions>\
                    {}\
                </Subscriptions>\
            </ListSubscriptionsResult>\
            <ResponseMetadata>\
                <RequestId>{}</RequestId>\
            </ResponseMetadata>\
        </ListSubscriptionsResponse>",
        subscription_xml,
        get_new_id(),
    );
    Ok(output)
}

pub fn list_subscriptions_by_topic(
    form: HashMap<String, String>,
    state: Arc<RwLock<State>>,
) -> MyResult<String> {
    let topic_arn = form
        .get("TopicArn")
        .ok_or_else(|| MyError::MissingParameter("TopicArn".to_string()))?;

    let s = state.read()?;

    let arn = TopicArn(topic_arn.clone());
    if let Some(t) = s.topics.get(&arn) {
        let mut subscription_xml = String::new();
        for sub in &t.subscriptions {
            subscription_xml.push_str(&sub.get_subscription_xml());
        }

        let output = format!(
            "<ListSubscriptionsByTopicResponse>\
                <ListSubscriptionsByTopicResult>\
                    <Subscriptions>\
                        {}\
                    </Subscriptions>\
                </ListSubscriptionsByTopicResult>\
                <ResponseMetadata>\
                    <RequestId>{}</RequestId>\
                </ResponseMetadata>\
            </ListSubscriptionsByTopicResponse>",
            subscription_xml,
            get_new_id(),
        );
        Ok(output)
    } else {
        Err(MyError::TopicNotFound(topic_arn.clone()))
    }
}
