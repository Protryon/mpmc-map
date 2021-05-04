# mpmc-map

## Purpose

This crate provides a rapidly-eventually-consistent multi-producer multi-consumer map. It's intended for high read low write environments.

## Design

`MpmcMap` is backed by an `arc-swap` of `im::HashMap`. We use an actor model to receive events through an `tokio::sync::mpsc` channel to an updater task that will apply updates and update the inner map.

## When you should not use this crate

This crate will have high overhead in high-write environments. Look for a debounced eventually consistent map implementation, perhaps `evmap`, if you have lots of writes.

Or of course, submit a PR to add that functionality here!


