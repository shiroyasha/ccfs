use crate::{server_config::ServerConfig, ws::GetAddress, CCFSRaft, Cluster};
use actix::Addr;
use actix_service::{Service, Transform};
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::{error, http, web::Data, Error, HttpResponse};
use core::task::{Context, Poll};
use futures::future::{ok, Future, Ready};
use std::cell::RefCell;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

pub struct RedirectToLeader;

impl<S> Transform<S, ServiceRequest> for RedirectToLeader
where
    S: Service<ServiceRequest, Response = ServiceResponse, Error = Error> + 'static,
    S::Future: 'static,
{
    type Response = ServiceResponse;
    type Error = Error;
    type InitError = ();
    type Transform = RedirectToLeaderMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(RedirectToLeaderMiddleware {
            service: Rc::new(RefCell::new(service)),
        })
    }
}
pub struct RedirectToLeaderMiddleware<S>
where
    S: Service<ServiceRequest>,
{
    service: Rc<RefCell<S>>,
}

impl<S> Service<ServiceRequest> for RedirectToLeaderMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse, Error = Error> + 'static,
    S::Future: 'static,
{
    type Response = ServiceResponse;
    type Error = Error;
    #[allow(clippy::type_complexity)]
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&self, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.service.borrow_mut().poll_ready(cx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let srv = self.service.clone();
        Box::pin(async move {
            let raft_node = req.app_data::<Data<Arc<CCFSRaft>>>().unwrap();
            let cluster = req.app_data::<Data<Addr<Cluster>>>().unwrap();
            let config = req.app_data::<Data<Arc<ServerConfig>>>().unwrap();
            match raft_node.current_leader().await {
                Some(leader_id) => {
                    if leader_id == config.id {
                        println!("executing req");
                        return srv.call(req).await;
                    }

                    match cluster.send(GetAddress { id: leader_id }).await {
                        Ok(Some(leader_address)) => {
                            let redirect_addr = format!("{}{}", leader_address, req.path());
                            println!("redirecting to {}", redirect_addr);
                            Ok(req.into_response(
                                HttpResponse::TemporaryRedirect()
                                    .insert_header((http::header::LOCATION, redirect_addr))
                                    .finish(),
                            ))
                        }
                        _ => Err(error::ErrorInternalServerError(
                            "Service is currently unavailable",
                        )),
                    }
                }
                None => Err(error::ErrorInternalServerError(
                    "Service is currently unavailable",
                )),
            }
        })
    }
}
