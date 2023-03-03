# vurstx
Rust TCP Client for Vertx EventBus Bridge

## Goal
This project aims to provide an efficient, non-blocking TCP Client for Vertx EventBus Bridge.
It only uses Rust **standard** library for the TCP stack with a thread pool implementation for
concurrent message processing.

## Current Status
The project is not a library. It is an executable binary that can register to an Vertx Eventbus
(hosted in port 7000) **address** and listen incoming messages.
