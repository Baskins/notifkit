import { EventEmitter } from "node:events";

export const globalEmitter = new EventEmitter();
// Every open SSE stream on the API listens here, so the count grows with
// connected clients rather than signalling a leak.
globalEmitter.setMaxListeners(0);
