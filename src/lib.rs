/*!
`actix_web` middleware for logging Stackdriver-compatible
[`LogEntry`](https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry) to `stdout`
*/
#![deny(missing_docs)]
use actix_web::{
    body::BodySize,
    dev::{MessageBody, ResponseBody, Service, ServiceRequest, ServiceResponse, Transform},
    http::{header::REFERER, Method, StatusCode},
    web::Bytes,
};
use chrono::{DateTime, Utc};
use futures::{future, Future};
use serde::{Serialize, Serializer};
use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

/// Custom serializers for those types that don't impl Serialize themselves
struct Serializers;

impl Serializers {
    fn to_rfc3339<S>(time: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&time.to_rfc3339())
    }

    fn to_u16<S>(status: &StatusCode, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u16(status.as_u16())
    }

    fn to_string<S, T>(field: T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: ToString,
    {
        serializer.serialize_str(&field.to_string())
    }
}

/// Log levels parsed by fluentd/Stackdriver
#[derive(Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum Level {
    Info,
    Warn,
    Error,
}

impl From<&StatusCode> for Level {
    fn from(status: &StatusCode) -> Self {
        let code = status.as_u16();

        if code < 400 {
            Self::Info
        } else if code >= 400 && code < 500 {
            Self::Warn
        } else {
            Self::Error
        }
    }
}

/// Structured HttpRequest payload type for fluentd/Stackdriver
// https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry#HttpRequest
#[derive(Serialize)]
struct HttpDescriptors {
    latency: String, // in X.Xs format
    #[serde(flatten)]
    request: RequestDescriptors,
    #[serde(flatten)]
    response: ResponseDescriptors,
}

/// HTTP descriptors related to incoming requests
// FIXME: remove the Clone impl
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct RequestDescriptors {
    #[serde(skip_serializing_if = "Option::is_none")]
    referer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    remote_ip: Option<String>,
    #[serde(serialize_with = "Serializers::to_string")]
    request_method: Method,
    request_url: String, // TODO: borrow instead
    #[serde(serialize_with = "Serializers::to_rfc3339")]
    time: DateTime<Utc>,
}

impl From<&ServiceRequest> for RequestDescriptors {
    fn from(request: &ServiceRequest) -> Self {
        let request_method = request.method().to_owned();
        let request_url = request.path().to_owned();
        let headers = request.headers();
        let time = Utc::now();

        // TODO: consider passing references instead of taking ownership of these values
        let remote_ip = request.connection_info().remote().map(String::from);
        let referer = headers.get(REFERER).and_then(|header| {
            if let Ok(valid_header) = header.to_str() {
                Some(valid_header.to_string())
            } else {
                None
            }
        });

        Self {
            referer,
            remote_ip,
            request_method,
            request_url,
            time,
        }
    }
}

/// HTTP descriptors related to incoming requests
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ResponseDescriptors {
    #[serde(skip_serializing)]
    time: DateTime<Utc>, // in X.Xs format TODO
    #[serde(serialize_with = "Serializers::to_u16")]
    status: StatusCode,
}

impl<B> From<&ServiceResponse<B>> for ResponseDescriptors {
    fn from(response: &ServiceResponse<B>) -> Self {
        let status = response.status();
        let time = Utc::now();

        Self { status, time }
    }
}

/// Structured Payload with fields recognized by fluend/Stackdriver
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Log<'a> {
    // TODO: use tracing instead once nested fields enabled
    // https://github.com/tokio-rs/tracing/issues/663
    http_request: &'a HttpDescriptors,
    severity: Level,
    #[serde(serialize_with = "Serializers::to_rfc3339")]
    time: &'a DateTime<Utc>,
}

/// `actix_web` middleware for transforming hyper services into logs to stdout.
pub struct RequestLogger;

impl<S, B> Transform<S> for RequestLogger
where
    S: Service<Request = ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error>
        + 'static,
    B: MessageBody + 'static,
{
    type Request = ServiceRequest;
    type Response = ServiceResponse<LogMessage<B>>;
    type Error = actix_web::Error;
    type InitError = ();
    type Transform = LoggerMiddleware<S>;
    type Future = future::Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        future::ok(LoggerMiddleware { service })
    }
}

/// Service that intercepts other Services for request and response parsing
pub struct LoggerMiddleware<S> {
    service: S,
}

impl<S, B> Service for LoggerMiddleware<S>
where
    S: Service<Request = ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    type Request = ServiceRequest;
    type Response = ServiceResponse<LogMessage<B>>;
    type Error = actix_web::Error;
    type Future = LoggerResponse<S, B>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(context)
    }

    fn call(&mut self, request: Self::Request) -> Self::Future {
        let request_descriptors = RequestDescriptors::from(&request);

        LoggerResponse {
            future: self.service.call(request),
            request_descriptors,
            _t: PhantomData,
        }
    }
}

/// Wrapped future that passes data through to the log parser
#[pin_project::pin_project]
pub struct LoggerResponse<S, B>
where
    B: MessageBody,
    S: Service,
{
    #[pin]
    future: S::Future,
    request_descriptors: RequestDescriptors,
    _t: PhantomData<B>,
}

impl<S, B> Future for LoggerResponse<S, B>
where
    B: MessageBody,
    S: Service<Request = ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error>,
{
    type Output = Result<ServiceResponse<LogMessage<B>>, actix_web::Error>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let projected = self.project();
        let response = futures::ready!(projected.future.poll(context));
        let request_descriptors = projected.request_descriptors.clone(); // TODO: avoid this

        Poll::Ready(response.map(|response| {
            let response_descriptors = ResponseDescriptors::from(&response);
            let milliseconds_latency = response_descriptors
                .time
                .signed_duration_since(request_descriptors.time)
                .num_milliseconds();
            let seconds_latency = milliseconds_latency / 1000;
            let latency = format!("{}s", &seconds_latency);

            let http_descriptors = HttpDescriptors {
                latency,
                response: response_descriptors,
                request: request_descriptors,
            };

            response.map_body(move |_, body| {
                ResponseBody::Body(LogMessage {
                    body,
                    http_descriptors,
                })
            })
        }))
    }
}

/// The message emitted by this middleware through a `ServiceResponse`
pub struct LogMessage<B> {
    body: ResponseBody<B>,
    http_descriptors: HttpDescriptors,
}

impl<B> Drop for LogMessage<B> {
    fn drop(&mut self) {
        let severity = Level::from(&self.http_descriptors.response.status);
        let time = &self.http_descriptors.response.time;
        let log = Log {
            http_request: &self.http_descriptors,
            severity,
            time,
        };

        if let Ok(message) = serde_json::to_string(&log) {
            println!("{}", &message);
        }
    }
}

impl<B: MessageBody> MessageBody for LogMessage<B> {
    fn size(&self) -> BodySize {
        self.body.size()
    }

    fn poll_next(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, actix_web::Error>>> {
        match self.body.poll_next(context) {
            Poll::Ready(Some(Ok(chunk))) => Poll::Ready(Some(Ok(chunk))),
            poll_state => poll_state,
        }
    }
}
