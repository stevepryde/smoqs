use crate::sqs::{
    create_queue, delete_queue, get_queue_attributes, list_queues, receive_message, send_message,
    set_queue_attributes,
};
use crate::state::State;

use env_logger::Env;
use log::{debug, info};

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::RwLock;
use structopt::StructOpt;

use crate::errors::MyError;
use crate::sns::{
    create_topic, delete_topic, get_topic_attributes, list_topics, publish, set_topic_attributes,
    subscribe, unsubscribe,
};
use warp::http::Response;
use warp::Filter;

mod errors;
mod misc;
mod sns;
mod sqs;
mod state;
mod xml;

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

#[derive(Debug, StructOpt)]
#[structopt(name = "SmoQS", about = "A quick and dirty SNS/SQS mock")]
pub struct Opt {
    /// The port to listen on. Default is 3566.
    #[structopt(short, long, env = "SMOQS_PORT")]
    port: Option<u16>,

    /// The default AWS region. Default is ap-southeast-2.
    #[structopt(long, env = "SMOQS_REGION")]
    region: Option<String>,

    #[structopt(long, env = "SMOQS_ACCOUNTID")]
    account: Option<String>,
}

#[tokio::main]
async fn main() {
    println!("SmoQS Version {}", VERSION);
    println!("-------------------");
    env_logger::from_env(Env::default().default_filter_or("smoqs=debug")).init();
    let opt = Opt::from_args();

    // Prefer CLI arg, otherwise environment variable, otherwise 4444.
    let port: u16 = opt.port.unwrap_or(3566);
    if port < 1024 {
        println!("Invalid port: {}", port);
        std::process::exit(1);
    }

    let region = opt.region.unwrap_or_else(|| "ap-southeast-2".to_string());
    let account_id = opt.account.unwrap_or_else(|| "000000000000".to_string());

    let addr: SocketAddr = match format!("127.0.0.1:{}", port).parse() {
        Ok(x) => x,
        Err(e) => {
            println!("Unable to access port: {:?}", e);
            std::process::exit(1);
        }
    };

    // Set up state.
    let state: Arc<RwLock<State>> = Arc::new(RwLock::new(State::new(port, &region, &account_id)));
    let state_filter = warp::any().map(move || state.clone());

    // Routes.
    let healthz = warp::path!("healthz").map(|| "OK".to_string());

    // All SNS/SQS requests come via forms.
    let root_post_form = warp::post()
        .and(warp::body::content_length_limit(1024 * 1024 * 2))
        .and(warp::body::form())
        .and(state_filter.clone())
        .map(|f: HashMap<String, String>, state| match f.get("Action") {
            Some(action) => {
                info!("ACTION: {}: {:?}", action, f);
                let result = match action.as_str() {
                    // SQS.
                    "ListQueues" => list_queues(f, state),
                    "CreateQueue" => create_queue(f, state),
                    "DeleteQueue" => delete_queue(f, state),
                    "GetQueueAttributes" => get_queue_attributes(f, state),
                    "SetQueueAttributes" => set_queue_attributes(f, state),
                    "SendMessage" => send_message(f, state),
                    "ReceiveMessage" => receive_message(f, state),
                    // SNS.
                    "ListTopics" => list_topics(f, state),
                    "CreateTopic" => create_topic(f, state),
                    "DeleteTopic" => delete_topic(f, state),
                    "GetTopicAttributes" => get_topic_attributes(f, state),
                    "SetTopicAttributes" => set_topic_attributes(f, state),
                    "Publish" => publish(f, state),
                    "Subscribe" => subscribe(f, state),
                    "Unsubscribe" => unsubscribe(f, state),
                    x => Err(MyError::UnknownAction(x.to_string())),
                };

                match result {
                    Ok(x) => {
                        debug!("Response:\n{}", x);
                        Response::builder().status(200).body(x)
                    }
                    Err(e) => Response::builder().status(400).body(e.get_error_response()),
                }
            }
            None => Response::builder()
                .status(404)
                .body(MyError::MissingAction.get_error_response()),
        });

    info!("Server running at {}", addr);
    warp::serve(healthz.or(root_post_form)).run(addr).await;
}
