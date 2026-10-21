import { describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { SWRConfig } from "swr";
import type { ReactNode } from "react";
import { ToastProvider } from "@/components/ui";
import { SensorsTab, type Op } from "@/components/SensorsTab";
import { SettingsTab } from "@/components/SettingsTab";
import StatusPage from "@/app/status/page";
import { request } from "@/lib/api";

const ID = 9900003;
const wrap = (ui: ReactNode) => render(<SWRConfig value={{ provider: () => new Map(), dedupingInterval: 0 }}><ToastProvider>{ui}</ToastProvider></SWRConfig>);

describe("Data Management: sensor CRUD through the UI writes to MongoDB", () => {
  it("creates, edits, changes status and deletes a sensor", async () => {
    await request(`/api/v1/sensors/${ID}`, { method: "DELETE" }).catch(() => undefined);
    const ops: Op[] = [];
    const user = userEvent.setup();
    wrap(<SensorsTab log={(o) => ops.push(o)} focusId={null} />);
    await screen.findByText(/1–\d+ of \d+/);

    // create
    await user.click(screen.getByRole("button", { name: "+ Add sensor" }));
    await user.type(screen.getByLabelText("Sensor id"), String(ID));
    await user.type(screen.getByLabelText("Latitude"), "37.4");
    await user.type(screen.getByLabelText("Longitude"), "-122.2");
    await user.type(screen.getByLabelText("Name"), "vitest ui sensor");
    await user.type(screen.getByLabelText("Tags"), "vitest, ui");
    await user.click(screen.getByRole("button", { name: "Create sensor" }));
    await waitFor(async () => expect((await request<{ name: string }>(`/api/v1/sensors/${ID}`)).data.name).toBe("vitest ui sensor"));
    expect(ops.at(-1)).toMatchObject({ method: "POST", status: 201 });

    // edit
    await user.clear(screen.getByLabelText("Search sensors"));
    await user.type(screen.getByLabelText("Search sensors"), String(ID));
    await user.click(await screen.findByRole("button", { name: `Edit sensor ${ID}` }));
    const name = screen.getByLabelText("Name");
    await user.clear(name);
    await user.type(name, "vitest ui sensor v2");
    await user.click(screen.getByRole("button", { name: "Save changes" }));
    await waitFor(async () => expect((await request<{ name: string; version: number }>(`/api/v1/sensors/${ID}`)).data).toMatchObject({ name: "vitest ui sensor v2", version: 2 }));

    // inline status
    await user.selectOptions(await screen.findByLabelText(`Status of sensor ${ID}`), "maintenance");
    await waitFor(async () => expect((await request<{ status: string }>(`/api/v1/sensors/${ID}`)).data.status).toBe("maintenance"));

    // invalid input is rejected by validation before/at the API and nothing is stored
    await user.click(screen.getByRole("button", { name: "+ Add sensor" }));
    await user.type(screen.getByLabelText("Sensor id"), "abc");
    await user.click(screen.getByRole("button", { name: "Create sensor" }));
    expect(await screen.findAllByText(/positive integer|required|invalid|must/i)).not.toHaveLength(0);
    await user.click(screen.getByRole("button", { name: "Cancel" }));

    // delete
    await user.click(await screen.findByRole("button", { name: `Delete sensor ${ID}` }));
    await user.click(within(await screen.findByRole("dialog")).getByRole("button", { name: "Delete" }));
    await waitFor(async () => expect(await request(`/api/v1/sensors/${ID}`).catch((e) => e.status)).toBe(404));
    expect(ops.map((o) => o.method)).toEqual(expect.arrayContaining(["POST", "PATCH", "DELETE"]));
  });
});

describe("Data Management: analyst settings", () => {
  it("lists the live settings with their defaults", async () => {
    wrap(<SettingsTab log={vi.fn()} />);
    expect(await screen.findByText(/alert_threshold/)).toBeInTheDocument();
    expect(screen.getByText(/watchlist_size/)).toBeInTheDocument();
  });
});

describe("Analysis History & System Status", () => {
  it("shows live service health, every MongoDB collection and the run history", async () => {
    wrap(<StatusPage />);
    expect(await screen.findByRole("heading", { level: 1, name: /System Status/ })).toBeInTheDocument();
    for (const c of ["sensor_metadata", "analysis_results", "congestion_hotspots", "traffic_predictions", "model_evaluations", "stream_metrics", "pipeline_runs", "settings"]) {
      expect((await screen.findAllByText(c, {}, { timeout: 20000 })).length).toBeGreaterThan(0);
    }
    expect(await screen.findByText("ready", {}, { timeout: 20000 })).toBeInTheDocument();
    expect((await screen.findAllByText("succeeded", {}, { timeout: 20000 })).length).toBeGreaterThan(5);
  });
});
