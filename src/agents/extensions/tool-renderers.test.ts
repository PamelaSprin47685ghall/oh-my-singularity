import { describe, expect, test } from "bun:test";

import { renderToolCall, renderToolResult } from "./tool-renderers";
import type { ToolTheme } from "./types";

const theme: ToolTheme = {
	fg: (_scope: string, text: string) => text,
	styledSymbol: () => "●",
	sep: { dot: "·" },
	spinnerFrames: ["⠋", "⠙", "⠹"],
};
// Runtime guard case: external renderer callers can still pass an invalid width.
const INVALID_WIDTH = undefined as unknown as number;

describe("tool renderers", () => {
	test("renderToolCall handles undefined width with Chinese summary text", () => {
		const component = renderToolCall("测试工具", () => [], theme, {
			isPartial: false,
			result: { content: [{ type: "text", text: "中文结果预览" }] },
		});

		const lines = component.render(INVALID_WIDTH);
		expect(lines.length).toBeGreaterThan(0);
		expect(lines[0]).toContain("测试工具");
		expect(lines[0]).toContain("中文结果预览");
	});

	test("renderToolResult handles undefined width with Chinese body text", () => {
		const component = renderToolResult(
			"测试工具",
			{ content: [{ type: "text", text: "第一行中文\n第二行中文" }] },
			{ expanded: false, isPartial: false },
			theme,
		);

		const lines = component.render(INVALID_WIDTH);
		expect(lines.length).toBeGreaterThan(0);
		expect(lines.join("\n")).toContain("第一行中文");
	});
});
