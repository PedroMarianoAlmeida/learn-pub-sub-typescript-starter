import amqp from "amqplib";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);

  console.log("Starting Peril server...");
  console.log("Connected to RabbitMQ.");

  await new Promise<void>((resolve) => {
    process.once("SIGINT", resolve);
  });

  console.log("Shutting down...");
  await conn.close();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
