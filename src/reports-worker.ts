// src/reports-worker.ts
export interface Env {
  DB: D1Database; // D1 binding
}

type CompanyReportTotals = {
  views_profile: number;
  impressions_search: number;
  impressions_map: number;
  clicks_phone: number;
  clicks_site: number;
  clicks_email: number;
  clicks_directions: number;
  leads_form: number;
};

type CompanyMeta = {
  id: number;
  slug: string;
  name: string;
  city_name: string | null;
  city_slug: string | null;
  country_code: string | null;
  primary_category_slug: string | null;
  plan_type: string | null;
};

type CompanyReport = {
  company: CompanyMeta;
  range: {
    from: string; // YYYY-MM-DD
    to: string; // YYYY-MM-DD
    days: number;
  };
  last_30_days: {
    totals: CompanyReportTotals;
    daily: Array<
      {
        date: string;
      } & CompanyReportTotals
    >;
  };
};

type SummaryItem = {
  company: CompanyMeta;
  totals: CompanyReportTotals;
};

type SummaryResponse = {
  range: {
    from: string;
    to: string;
    days: number;
  };
  items: SummaryItem[];
};

// 🔹 Типове събития, които ще броим (само за /track)
type EventType =
  | "view_profile"
  | "impression_search"
  | "impression_map"
  | "click_phone"
  | "click_site"
  | "click_email"
  | "click_directions"
  | "lead_form";

function emptyTotals(): CompanyReportTotals {
  return {
    views_profile: 0,
    impressions_search: 0,
    impressions_map: 0,
    clicks_phone: 0,
    clicks_site: 0,
    clicks_email: 0,
    clicks_directions: 0,
    leads_form: 0,
  };
}

function jsonResponse(body: unknown, init: ResponseInit = {}): Response {
  const headers = new Headers(init.headers);
  headers.set("content-type", "application/json; charset=utf-8");
  headers.set("access-control-allow-origin", "*");
  headers.set("access-control-allow-methods", "GET, POST, OPTIONS");
  headers.set("access-control-allow-headers", "Content-Type");
  return new Response(JSON.stringify(body), {
    ...init,
    headers,
  });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const { searchParams } = url;

    console.log("[reports-worker] incoming", {
      method: request.method,
      url: url.toString(),
      pathname: url.pathname,
      search: url.search,
    });

    // НОРМАЛИЗИРАН ПЪТ (важно за /api/reports/*)
    let routePath = url.pathname;

    if (routePath.startsWith("/api/reports")) {
      routePath = routePath.slice("/api/reports".length) || "/";
    }

    if (!routePath.startsWith("/")) {
      routePath = "/" + routePath;
    }

    console.log("[reports-worker] normalized routePath:", routePath);

    if (request.method === "OPTIONS") {
      return jsonResponse({}, { status: 204 });
    }

    // ---------- 1) TRACK ENDPOINT (POST /track) – legacy ----------
    if (request.method === "POST" && routePath === "/track") {
      try {
        const body = (await request.json().catch(() => null)) as
          | { slug?: string; type?: EventType }
          | null;

        console.log("[reports-worker] /track body:", body);

        const slug = body?.slug?.trim();
        const type = body?.type;

        if (!slug || !type) {
          console.warn("[reports-worker] /track invalid payload", body);
          return jsonResponse(
            { error: "Missing slug or type" },
            { status: 400 },
          );
        }

        // map event type -> column
        const columnMap: Record<EventType, keyof CompanyReportTotals> = {
          view_profile: "views_profile",
          impression_search: "impressions_search",
          impression_map: "impressions_map",
          click_phone: "clicks_phone",
          click_site: "clicks_site",
          click_email: "clicks_email",
          click_directions: "clicks_directions",
          lead_form: "leads_form",
        };

        const column = columnMap[type];
        if (!column) {
          console.warn("[reports-worker] unknown event type", type);
          return jsonResponse({ error: "Unknown event type" }, { status: 400 });
        }

        // търсим company по slug
        const companyRow = await env.DB.prepare(
          "SELECT id, slug, name FROM companies WHERE slug = ?",
        )
          .bind(slug)
          .first<{ id: number; slug: string; name: string }>();

        if (!companyRow) {
          console.warn("[reports-worker] /track company not found", slug);
          return jsonResponse({ error: "Company not found" }, { status: 404 });
        }

        const companyId = companyRow.id;

        // дата (UTC) – деня на събитието
        const now = new Date();
        const dateStr = now.toISOString().slice(0, 10);

        console.log("[reports-worker] /track upsert", {
          slug,
          companyId,
          type,
          column,
          dateStr,
        });

        // INSERT ... ON CONFLICT ... DO UPDATE -> +1 в съответната колона
        await env.DB.prepare(
          `
          INSERT INTO company_daily_stats (company_id, date, ${column})
          VALUES (?, ?, 1)
          ON CONFLICT(company_id, date)
          DO UPDATE SET ${column} = ${column} + 1
          `,
        )
          .bind(companyId, dateStr)
          .run();

        return jsonResponse({ ok: true });
      } catch (err) {
        console.error("[reports-worker] /track error", err);
        return jsonResponse({ error: "Internal error" }, { status: 500 });
      }
    }

    // ---------- 2) GET REPORTS (основния use-case) ----------

    if (request.method !== "GET") {
      console.warn("[reports-worker] 405 for method", request.method);
      return jsonResponse({ error: "Method not allowed" }, { status: 405 });
    }

    // по default 30 дни назад
    const daysParam = Number(searchParams.get("days") || "30");
    const days = Number.isFinite(daysParam) && daysParam > 0 ? daysParam : 30;

    const now = new Date();
    const toDate = new Date(
      Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()),
    );
    const fromDate = new Date(
      toDate.getTime() - (days - 1) * 24 * 60 * 60 * 1000,
    );

    const fromStr = fromDate.toISOString().slice(0, 10);
    const toStr = toDate.toISOString().slice(0, 10);

    console.log("[reports-worker] computed range", {
      days,
      fromStr,
      toStr,
    });

    // --- /company/:slug ---
    if (routePath.startsWith("/company/")) {
      const slug = decodeURIComponent(
        routePath.replace("/company/", "").trim(),
      );

      console.log("[reports-worker] /company/:slug hit", { slug });

      if (!slug) {
        console.warn("[reports-worker] invalid empty slug");
        return jsonResponse(
          { error: "Invalid company slug" },
          { status: 400 },
        );
      }

      const companyRow = await env.DB.prepare(
        `
        SELECT
          id,
          slug,
          name,
          city_name,
          city_slug,
          country_code,
          primary_category_slug,
          plan_type
        FROM companies
        WHERE slug = ?
        `,
      )
        .bind(slug)
        .first<{
          id: number;
          slug: string;
          name: string;
          city_name: string | null;
          city_slug: string | null;
          country_code: string | null;
          primary_category_slug: string | null;
          plan_type: string | null;
        }>();

      console.log("[reports-worker] companyRow", companyRow);

      if (!companyRow) {
        console.warn("[reports-worker] company not found for slug", slug);
        return jsonResponse({ error: "Company not found" }, { status: 404 });
      }

      const companyId = companyRow.id;

      const statsResult = await env.DB.prepare(
        `
        SELECT
          date,
          views_profile,
          impressions_search,
          impressions_map,
          clicks_phone,
          clicks_site,
          clicks_email,
          clicks_directions,
          leads_form
        FROM company_daily_stats
        WHERE company_id = ?
          AND date >= ?
          AND date <= ?
        ORDER BY date ASC
        `,
      )
        .bind(companyId, fromStr, toStr)
        .all();

      const rows = (statsResult.results as Array<any> | undefined) ?? [];

      console.log("[reports-worker] stats rows", {
        companyId,
        count: rows.length,
      });

      const totals = emptyTotals();
      const daily: CompanyReport["last_30_days"]["daily"] = [];

      for (const row of rows) {
        const item = {
          date: row.date as string,
          views_profile: Number(row.views_profile ?? 0),
          impressions_search: Number(row.impressions_search ?? 0),
          impressions_map: Number(row.impressions_map ?? 0),
          clicks_phone: Number(row.clicks_phone ?? 0),
          clicks_site: Number(row.clicks_site ?? 0),
          clicks_email: Number(row.clicks_email ?? 0),
          clicks_directions: Number(row.clicks_directions ?? 0),
          leads_form: Number(row.leads_form ?? 0),
        };

        daily.push(item);

        totals.views_profile += item.views_profile;
        totals.impressions_search += item.impressions_search;
        totals.impressions_map += item.impressions_map;
        totals.clicks_phone += item.clicks_phone;
        totals.clicks_site += item.clicks_site;
        totals.clicks_email += item.clicks_email;
        totals.clicks_directions += item.clicks_directions;
        totals.leads_form += item.leads_form;
      }

      console.log("[reports-worker] totals", totals);

      const company: CompanyMeta = {
        id: companyRow.id,
        slug: companyRow.slug,
        name: companyRow.name,
        city_name: companyRow.city_name,
        city_slug: companyRow.city_slug,
        country_code: companyRow.country_code,
        primary_category_slug: companyRow.primary_category_slug,
        plan_type: companyRow.plan_type,
      };

      const body: CompanyReport = {
        company,
        range: {
          from: fromStr,
          to: toStr,
          days,
        },
        last_30_days: {
          totals,
          daily,
        },
      };

      return jsonResponse(body);
    }

    // --- /summary ---
    if (routePath === "/summary") {
      console.log("[reports-worker] /summary hit");

      const result = await env.DB.prepare(
        `
        SELECT
          c.id,
          c.slug,
          c.name,
          c.city_name,
          c.city_slug,
          c.country_code,
          c.primary_category_slug,
          c.plan_type,
          COALESCE(SUM(s.views_profile), 0)      AS views_profile,
          COALESCE(SUM(s.impressions_search), 0) AS impressions_search,
          COALESCE(SUM(s.impressions_map), 0)    AS impressions_map,
          COALESCE(SUM(s.clicks_phone), 0)       AS clicks_phone,
          COALESCE(SUM(s.clicks_site), 0)        AS clicks_site,
          COALESCE(SUM(s.clicks_email), 0)       AS clicks_email,
          COALESCE(SUM(s.clicks_directions), 0)  AS clicks_directions,
          COALESCE(SUM(s.leads_form), 0)         AS leads_form
        FROM companies c
        LEFT JOIN company_daily_stats s
          ON s.company_id = c.id
         AND s.date >= ?
         AND s.date <= ?
        GROUP BY
          c.id,
          c.slug,
          c.name,
          c.city_name,
          c.city_slug,
          c.country_code,
          c.primary_category_slug,
          c.plan_type
        ORDER BY views_profile DESC
        `,
      )
        .bind(fromStr, toStr)
        .all();

      const rows = (result.results as Array<any> | undefined) ?? [];
      console.log("[reports-worker] /summary rows", { count: rows.length });

      const items: SummaryItem[] = rows.map((row) => ({
        company: {
          id: row.id as number,
          slug: row.slug as string,
          name: row.name as string,
          city_name: (row.city_name as string) ?? null,
          city_slug: (row.city_slug as string) ?? null,
          country_code: (row.country_code as string) ?? null,
          primary_category_slug:
            (row.primary_category_slug as string) ?? null,
          plan_type: (row.plan_type as string) ?? null,
        },
        totals: {
          views_profile: Number(row.views_profile ?? 0),
          impressions_search: Number(row.impressions_search ?? 0),
          impressions_map: Number(row.impressions_map ?? 0),
          clicks_phone: Number(row.clicks_phone ?? 0),
          clicks_site: Number(row.clicks_site ?? 0),
          clicks_email: Number(row.clicks_email ?? 0),
          clicks_directions: Number(row.clicks_directions ?? 0),
          leads_form: Number(row.leads_form ?? 0),
        },
      }));

      const body: SummaryResponse = {
        range: {
          from: fromStr,
          to: toStr,
          days,
        },
        items,
      };

      return jsonResponse(body);
    }

    console.warn("[reports-worker] 404 for routePath", routePath);
    return jsonResponse({ error: "Not found" }, { status: 404 });
  },
};
