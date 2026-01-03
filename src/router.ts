// src/router.ts
import metricsWorker from "./metrics-worker";
import reportsWorker from "./reports-worker";
import eventsConsumer from "./events-consumer";

// Ако искаш – можеш да си дефинираш Env/Batch по-строго.
// За да не се борим с типове сега – ползваме any.
export default {
  // 1) CRON → само към metricsWorker
  async scheduled(event: ScheduledEvent, env: any, ctx: ExecutionContext) {
    if (typeof (metricsWorker as any).scheduled === "function") {
      return (metricsWorker as any).scheduled(event, env, ctx);
    }
  },

  // 2) QUEUE consumer → events-consumer
  async queue(batch: any, env: any, ctx: ExecutionContext) {
    // тук директно делегираме към events-consumer
    if (typeof (eventsConsumer as any).queue === "function") {
      return (eventsConsumer as any).queue(batch, env, ctx);
    }

    console.warn("[router] queue() called but eventsConsumer.queue is missing");
  },

  // 3) HTTP заявки
  async fetch(request: Request, env: any, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const { hostname, pathname } = url;

    // --- METRICS DASHBOARD ---
    // metrics.firmlocator.com → metricsWorker
    if (hostname === "metrics.firmlocator.com") {
      return (metricsWorker as any).fetch(request, env, ctx);
    }

    // --- REPORTS по отделен субдомен ---
    // reports.firmlocator.com → reportsWorker
    if (hostname === "reports.firmlocator.com") {
      return (reportsWorker as any).fetch(request, env);
    }

    // --- REPORTS през api субпътя ---
    // api.firmlocator.com/api/reports/* → reportsWorker
    if (hostname === "api.firmlocator.com" && pathname.startsWith("/api/reports/")) {
      return (reportsWorker as any).fetch(request, env);
    }

    // fallback – всичко друго
    return new Response("Not found (router)", { status: 404 });
  },
};
