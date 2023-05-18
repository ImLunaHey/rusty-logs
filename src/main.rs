use axiom_rs::Client;
use serde_json::Value;
use std::env;
use std::io::{self, BufRead};

async fn ingest_to_axiom(client: &Client, dataset: &String, line: &String) {
    let json: Result<Value, serde_json::Error> = serde_json::from_str(&line);

    if json.is_ok() {
        // Send to axiom
        let result = client.ingest(dataset, vec![json.ok().unwrap()]).await;

        if result.is_err() {
            let error_message = match result.err() {
                Some(axiom_error) => match axiom_error {
                    axiom_rs::Error::Axiom(axiom_rs::error::AxiomError {
                        message: Some(message),
                        ..
                    }) => message,
                    _ => "Unknown error occurred".to_owned(),
                },
                _ => "Unknown error occurred".to_owned(),
            };

            // If sending to Axiom fails, print a JSON stringified line indicating the failure
            println!(
                "{}",
                serde_json::to_string(&serde_json::json!({
                    "error": {
                        "message": "Failed to send to Axiom"
                    },
                    "cause": error_message,
                    "log": &line
                }))
                .unwrap()
            );

            // Then print the line itself
            println!("{}", &line);
        }
    } else {
        // If parsing the JSON fails, print a JSON stringified line indicating the failure
        println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "error": {
                    "message": "Failed to parse line into JSON"
                },
                "cause": format!("{:?}", json.err()),
                "log": &line
            }))
            .unwrap()
        );

        // Then print the line itself
        println!("{}", &line);
    }
}

#[tokio::main]
async fn main() {
    // Check if envs are set
    let token = env::var("AXIOM_TOKEN").expect("Missing AXIOM_TOKEN env");
    let dataset = env::var("AXIOM_DATASET").expect("Missing AXIOM_DATASET env");

    // Create Axiom client
    let client = Client::builder()
        .with_token(token)
        .build()
        .expect("Failed to get axiom client");

    // Print that we're started
    println!("Starting to parse lines");

    // Get stdin
    let stdin = io::stdin();
    let handle = stdin.lock();

    // Send each line to axiom
    for line in handle.lines() {
        if let Ok(line) = line {
            ingest_to_axiom(&client, &dataset, &line).await;
        }
    }

    // Print that we're stopping
    println!("Exiting process");
}
