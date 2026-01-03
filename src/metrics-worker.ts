// src/metrics-worker.ts

const GRAPHQL_ENDPOINT = "https://api.cloudflare.com/client/v4/graphql";
const METRICS_KEY = "firmlocator:homepage-metrics";

export interface Env {
  CF_API_TOKEN: string;        // Worker secret
  CF_ZONE_TAG: string;         // VAR (zone id)
  METRICS_TOTAL: KVNamespace;  // KV binding
}

type HttpGroup = {
  sum?: {
    requests?: number;
    bytes?: number;
    cachedRequests?: number;
    threats?: number;
  };
  uniq?: {
    uniques?: number;
  };
};

type GraphQLResponse = {
  errors?: { message: string }[];
  data?: {
    viewer?: {
      zones?: Array<{
        httpRequestsAdaptiveGroups?: HttpGroup[];
      }>;
    };
  };
};

export type MetricsPayload = {
  updatedAt: string;
  from: string;
  to: string;
  totals: {
    requests: number;
    uniques: number;
    bytes: number;
    threats: number;
    cachedRequests: number;
    cacheHitRate: number; // 0..1
  };
};

async function fetchZoneMetrics(env: Env): Promise<MetricsPayload> {
  const now = new Date();

  // 30 дни назад
  const fromDate = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);

  // httpRequests1dGroups работи с Date (YYYY-MM-DD), не с Time
  const to = now.toISOString().slice(0, 10);       // напр. "2025-12-14"
  const from = fromDate.toISOString().slice(0, 10); // напр. "2025-11-14"

  const query = `
    query GetZoneMetrics($zoneTag: String!, $from: Date!, $to: Date!) {
      viewer {
        zones(filter: { zoneTag: $zoneTag }) {
          httpRequests1dGroups(
            limit: 400
            filter: { date_geq: $from, date_lt: $to }
          ) {
            sum {
              requests
              bytes
              cachedRequests
              threats
            }
            uniq {
              uniques
            }
          }
        }
      }
    }
  `;

  const body = JSON.stringify({
    query,
    variables: {
      zoneTag: env.CF_ZONE_TAG,
      from,
      to,
    },
  });

  const res = await fetch(GRAPHQL_ENDPOINT, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${env.CF_API_TOKEN}`,
    },
    body,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`GraphQL HTTP error ${res.status}: ${text}`);
  }

  const json = (await res.json()) as GraphQLResponse;

  if (json.errors && json.errors.length) {
    throw new Error(
      "GraphQL returned errors: " +
        json.errors.map((e) => e.message).join("; "),
    );
  }

  const groups =
    json.data?.viewer?.zones?.[0]?.httpRequests1dGroups ?? [];

  let requests = 0;
  let bytes = 0;
  let cachedRequests = 0;
  let threats = 0;
  let uniques = 0;

  for (const g of groups) {
    if (g.sum) {
      requests += g.sum.requests ?? 0;
      bytes += g.sum.bytes ?? 0;
      cachedRequests += g.sum.cachedRequests ?? 0;
      threats += g.sum.threats ?? 0;
    }
    if (g.uniq) {
      uniques += g.uniq.uniques ?? 0;
    }
  }

  const cacheHitRate = requests > 0 ? cachedRequests / requests : 0;

  return {
    updatedAt: now.toISOString(),
    from,
    to,
    totals: {
      requests,
      uniques,
      bytes,
      threats,
      cachedRequests,
      cacheHitRate,
    },
  };
}

async function updateMetrics(env: Env): Promise<void> {
  const payload = await fetchZoneMetrics(env);
  await env.METRICS_TOTAL.put(METRICS_KEY, JSON.stringify(payload));
}

function jsonResponse(body: unknown, init: ResponseInit = {}): Response {
  const headers = new Headers(init.headers);
  headers.set("content-type", "application/json; charset=utf-8");
  headers.set("access-control-allow-origin", "*");
  headers.set("access-control-allow-methods", "GET, OPTIONS");
  headers.set("access-control-allow-headers", "Content-Type");
  return new Response(JSON.stringify(body), {
    ...init,
    headers,
  });
}

export default {
  // cron trigger – 1x дневно е достатъчно
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    ctx.waitUntil(
      updateMetrics(env).catch((err) => {
        console.error("scheduled updateMetrics error", err);
      }),
    );
  },

  // HTTP endpoint за Next.js / admin / каквото искаш
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    if (request.method === "OPTIONS") {
      return jsonResponse({}, { status: 204 });
    }

    // /metrics/latest или /latest (ако решиш друг route)
    if (path === "/latest" || path.endsWith("/metrics/latest")) {
      const raw = await env.METRICS_TOTAL.get(METRICS_KEY, "text");
      if (!raw) {
        return jsonResponse(
          { error: "metrics not ready yet" },
          { status: 503 },
        );
      }
      const headers = new Headers();
      headers.set("content-type", "application/json; charset=utf-8");
      headers.set("access-control-allow-origin", "*");
      headers.set("access-control-allow-methods", "GET, OPTIONS");
      headers.set("access-control-allow-headers", "Content-Type");
      return new Response(raw, { headers });
    }

    if (path === "/health" || path.endsWith("/metrics/health")) {
      return new Response("ok");
    }

    return new Response("Not found", { status: 404 });
  },
};
