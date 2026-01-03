// src/events-consumer.ts
import type { BaseEnv, QueueEvent, EventType } from "../../worker/src/core/types";

// Тук не ти трябва EVENTS_QUEUE, само DB.

export default {
  async queue(batch: any, env: BaseEnv): Promise<void> {
    const messages: any[] = batch.messages || [];
    console.log("[events-consumer] batch size =", messages.length);

    const aggregates: Record<
      string,
      {
        companyId: number;
        date: string;
        views_profile: number;
        impressions_search: number;
        impressions_map: number;
        clicks_phone: number;
        clicks_site: number;
        clicks_email: number;
        clicks_directions: number;
        leads_form: number;
      }
    > = {};

    for (const msg of messages) {
      const e = msg.body as QueueEvent;

      console.log("[events-consumer] received event", {
        companyId: e.companyId,
        type: e.type,
        createdAt: e.createdAt,
        hasMeta: !!e.meta,
      });

      if (!e.companyId || !e.createdAt) {
        console.warn("[events-consumer] skipping event with missing companyId/createdAt", e);
        continue;
      }

      const date = e.createdAt.slice(0, 10); // YYYY-MM-DD
      const key = `${e.companyId}:${date}`;

      if (!aggregates[key]) {
        aggregates[key] = {
          companyId: e.companyId,
          date,
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

      const agg = aggregates[key];

      switch (e.type as EventType) {
        case "PROFILE_VIEW":
          agg.views_profile++;
          break;
        case "SEARCH_IMPRESSION":
          agg.impressions_search++;
          break;
        case "MAP_IMPRESSION":
          agg.impressions_map++;
          break;
        case "CLICK_PHONE":
          agg.clicks_phone++;
          break;
        case "CLICK_SITE":
          agg.clicks_site++;
          break;
        case "CLICK_EMAIL":
          agg.clicks_email++;
          break;
        case "CLICK_DIRECTIONS":
          agg.clicks_directions++;
          break;
        case "FORM_SUBMIT":
          agg.leads_form++;
          break;
        default:
          console.warn("[events-consumer] unknown event type, skipping", {
            type: e.type,
            companyId: e.companyId,
          });
      }
    }

    console.log(
      "[events-consumer] computed aggregates",
      Object.values(aggregates).map((a) => ({
        companyId: a.companyId,
        date: a.date,
        views_profile: a.views_profile,
        impressions_search: a.impressions_search,
        impressions_map: a.impressions_map,
        clicks_phone: a.clicks_phone,
        clicks_site: a.clicks_site,
        clicks_email: a.clicks_email,
        clicks_directions: a.clicks_directions,
        leads_form: a.leads_form,
      })),
    );

    const sql = `
      INSERT INTO company_daily_stats (
        company_id, date,
        views_profile, impressions_search, impressions_map,
        clicks_phone, clicks_site, clicks_email, clicks_directions,
        leads_form
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(company_id, date) DO UPDATE SET
        views_profile      = views_profile      + excluded.views_profile,
        impressions_search = impressions_search + excluded.impressions_search,
        impressions_map    = impressions_map    + excluded.impressions_map,
        clicks_phone       = clicks_phone       + excluded.clicks_phone,
        clicks_site        = clicks_site        + excluded.clicks_site,
        clicks_email       = clicks_email       + excluded.clicks_email,
        clicks_directions  = clicks_directions  + excluded.clicks_directions,
        leads_form         = leads_form         + excluded.leads_form
    `;

    try {
      for (const key of Object.keys(aggregates)) {
        const agg = aggregates[key];

        console.log("[events-consumer] upserting stats row", {
          companyId: agg.companyId,
          date: agg.date,
          views_profile: agg.views_profile,
          impressions_search: agg.impressions_search,
          impressions_map: agg.impressions_map,
          clicks_phone: agg.clicks_phone,
          clicks_site: agg.clicks_site,
          clicks_email: agg.clicks_email,
          clicks_directions: agg.clicks_directions,
          leads_form: agg.leads_form,
        });

        await env.DB.prepare(sql)
          .bind(
            agg.companyId,
            agg.date,
            agg.views_profile,
            agg.impressions_search,
            agg.impressions_map,
            agg.clicks_phone,
            agg.clicks_site,
            agg.clicks_email,
            agg.clicks_directions,
            agg.leads_form,
          )
          .run();
      }

      // Ack-ваме всички съобщения чак след успешния запис
      for (const msg of messages) {
        msg.ack();
      }

      console.log("[events-consumer] acked messages =", messages.length);
    } catch (err) {
      console.error("[events-consumer] DB error, NOT acking messages", err);
      // хвърляме грешка, за да може queue-то да retry-не
      throw err;
    }
  },
};
