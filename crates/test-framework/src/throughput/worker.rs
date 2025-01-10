/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use anyhow::Result;
use flight_client::FlightClient;
use futures::StreamExt;
use tokio::task::JoinHandle;

use super::EndCondition;

pub(crate) struct ThroughputQueryWorker {
    id: usize,
    query_set: Vec<(&'static str, &'static str)>,
    end_condition: EndCondition,
    flight_client: FlightClient,
}

pub struct ThroughputQueryWorkerResult {
    pub query_durations: BTreeMap<String, Vec<Duration>>,
    pub connection_failed: bool,
    pub row_counts: BTreeMap<String, usize>,
}

impl ThroughputQueryWorkerResult {
    pub fn new(
        query_durations: BTreeMap<String, Vec<Duration>>,
        connection_failed: bool,
        row_counts: BTreeMap<String, usize>,
    ) -> Self {
        Self {
            query_durations,
            connection_failed,
            row_counts,
        }
    }
}

impl ThroughputQueryWorker {
    pub fn new(
        id: usize,
        query_set: Vec<(&'static str, &'static str)>,
        end_condition: EndCondition,
        flight_client: FlightClient,
    ) -> Self {
        Self {
            id,
            query_set,
            end_condition,
            flight_client,
        }
    }

    pub fn start(self) -> JoinHandle<Result<ThroughputQueryWorkerResult>> {
        tokio::spawn(async move {
            let mut query_durations: BTreeMap<String, Vec<Duration>> = BTreeMap::new();
            let mut row_counts: BTreeMap<String, usize> = BTreeMap::new();
            let mut query_set_count = 0;
            let start = Instant::now();

            while !self.end_condition.is_met(&start, query_set_count) {
                'query_set: for query in &self.query_set {
                    let mut row_count = 0;
                    let query_start = Instant::now();
                    match self.flight_client.query(query.1).await {
                        Ok(mut result_stream) => {
                            while let Some(batch) = result_stream.next().await {
                                match batch {
                                    Ok(batch) => {
                                        row_count += batch.num_rows();
                                    }
                                    Err(e) => {
                                        eprintln!(
                                            "FAIL - Worker {} - Query '{}' failed: {}",
                                            self.id, query.0, e
                                        );
                                        query_durations.entry(query.0.to_string()).or_default();
                                        continue 'query_set;
                                    }
                                }
                            }

                            let duration = query_start.elapsed();
                            query_durations
                                .entry(query.0.to_string())
                                .or_default()
                                .push(duration);

                            *row_counts.entry(query.0.to_string()).or_default() += row_count;
                        }
                        Err(e) => match e {
                            flight_client::Error::UnableToConnectToServer { .. }
                            | flight_client::Error::UnableToPerformHandshake { .. } => {
                                eprintln!(
                                    "FAIL - EARLY EXIT - Worker {} - Query '{}' failed: {}",
                                    self.id, query.0, e
                                );
                                return Ok(ThroughputQueryWorkerResult::new(
                                    query_durations,
                                    true,
                                    row_counts,
                                ));
                            }
                            _ => {
                                eprintln!(
                                    "FAIL - Worker {} - Query '{}' failed: {}",
                                    self.id, query.0, e
                                );
                                query_durations.entry(query.0.to_string()).or_default();
                            }
                        },
                    };
                }
                query_set_count += 1;
            }
            Ok(ThroughputQueryWorkerResult::new(
                query_durations,
                false,
                row_counts,
            ))
        })
    }
}
