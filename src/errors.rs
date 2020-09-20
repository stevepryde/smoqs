use crate::misc::get_new_id;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MyError {
    #[error("Missing action")]
    MissingAction,
    #[error("Unknown action: {0}")]
    UnknownAction(String),
    #[error("Missing parameter: {0}")]
    MissingParameter(String),
    #[error("Queue not found: {0}")]
    QueueNotFound(String),
    #[error("Topic not found: {0}")]
    TopicNotFound(String),
}

pub type MyResult<T> = Result<T, MyError>;

impl MyError {
    pub fn get_error_response(&self) -> String {
        format!(
            "<ErrorResponse>\
                <Error>\
                    <Type>Sender</Type>\
                    <Code>InvalidParameterValue</Code>\
                    <Message>{}</Message>\
                </Error>\
                <RequestId>{}</RequestId>\
            </ErrorResponse>",
            self.to_string(),
            get_new_id()
        )
    }
}
