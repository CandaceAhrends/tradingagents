import asyncio
import json
import logging
import ollama
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as redis

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("StockScannerGateway")

app = FastAPI(title="Stock Scanner AI & Real-Time Gateway", version="1.0.0")

# Enable CORS for your Angular 22 development server (typically running on port 4200)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4200",
        "http://127.0.0.1:4200",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# Redis Connection Configuration
REDIS_HOST = "localhost"
REDIS_PORT = 6379
STREAM_NAME = "top50:computed"  # written by pct_change_worker.py


async def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def execute_simulated_trade(symbol: str, side: str, quantity: int, reasoning: str) -> str:
    """Execute a simulated paper trade for a stock symbol."""
    return json.dumps({
        "event": "SIMULATED_TRADE",
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "reasoning": reasoning
    })


@app.websocket("/ws/tradingagent")
async def websocket_agent_endpoint(websocket: WebSocket):
    await websocket.accept()
    redis_client = await get_redis_client()
    ollama_client = ollama.AsyncClient()

    try:
        while True:
            # Grab only the single newest snapshot from Redis
            latest = await redis_client.xrevrange(STREAM_NAME, count=1)
            if latest:
                _, fields = latest[0]
                snapshot = fields.get("data")

                messages = [
                    {
                        "role": "system",
                        "content": "You are a trading agent. Analyze the scanner snapshot and call execute_simulated_trade only if a strong setup exists."
                    },
                    {"role": "user", "content": f"Current market snapshot: {snapshot}"}
                ]

                response = await ollama_client.chat(
                    model="qwen3-coder:30b",
                    messages=messages,
                    tools=[execute_simulated_trade]
                )

                # If Qwen triggers a trade tool call, run it and push to Angular
                if response.message.tool_calls:
                    for call in response.message.tool_calls:
                        trade_result = execute_simulated_trade(
                            **call.function.arguments)
                        await websocket.send_text(trade_result)
                else:
                    await websocket.send_json({
                        "event": "AGENT_HOLD",
                        "reasoning": response.message.content
                    })

            # Wait 60 seconds (or 300 for 5 minutes) before checking the next snapshot
            await asyncio.sleep(60)

    except WebSocketDisconnect:
        logger.info("Agent WebSocket client disconnected.")
    finally:
        await redis_client.close()


@app.websocket("/ws/top50")
async def websocket_scanner_endpoint(websocket: WebSocket):
    """
    Bi-directional WebSocket endpoint for streaming real-time stock ticks
    and multi-agent metrics directly to the Angular 22 AG Grid frontend.
    """
    await websocket.accept()
    logger.info("Angular 22 frontend client connected to WebSocket.")

    client = await get_redis_client()
    last_id = "$"  # Start listening only to new messages added to the stream after connection

    try:
        while True:
            # Read new entries from the Redis Stream non-blockingly (block for max 2000ms)
            streams = await client.xread({STREAM_NAME: last_id}, count=1, block=2000)

            if streams:
                for stream_name, messages in streams:
                    for message_id, fields in messages:
                        last_id = message_id
                        data_payload = fields.get("data")

                        if data_payload:
                            # Parse the JSON payload coming from the Rust / Mock producer
                            parsed_json = json.loads(data_payload)

                            # Broadcast down the WebSocket to Angular
                            await websocket.send_json(parsed_json)

            # Yield control back to the event loop to ensure high concurrency handling
            await asyncio.sleep(0.01)

    except WebSocketDisconnect:
        logger.info("Angular 22 frontend client disconnected.")
    except Exception as e:
        logger.error(f"Error in WebSocket bridge: {e}")
    finally:
        await client.aclose()
        logger.info("Redis connection closed for client session.")


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "FastAPI Stock Scanner Bridge"}
