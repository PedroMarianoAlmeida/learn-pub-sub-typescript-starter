import amqp from "amqplib";

async function main() {
    const rabbitConnString = "amqp://guest:guest@localhost:5672/";
    const conn = await amqp.connect(rabbitConnString);
    console.log("connection was successful")
    await conn.createConfirmChannel()
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
