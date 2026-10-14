import {
  Box,
  Checkbox,
  Flex,
  NativeSelect,
  Slider,
  Stack,
  Text,
} from "@chakra-ui/react";
import type { DatasetId } from "./datasets.js";
import { DATASETS, LULC_CLASSES } from "./datasets.js";
import { Field, ControlPanel as SharedControlPanel } from "./shared/index.js";
import type { StacItem } from "./stac.js";

/** Load state of one dataset's STAC items. */
export type DatasetState =
  | { status: "loading" }
  | { status: "error"; error: string }
  | { status: "loaded"; items: StacItem[] };

export interface ControlPanelProps {
  /** Which datasets are shown. */
  enabled: Record<DatasetId, boolean>;
  onEnabledChange: (id: DatasetId, enabled: boolean) => void;
  /** Layer opacity per dataset, 0 to 1. */
  opacity: Record<DatasetId, number>;
  onOpacityChange: (id: DatasetId, opacity: number) => void;
  /** Load state per dataset, absent until first enabled. */
  states: Partial<Record<DatasetId, DatasetState>>;
  /** Years available across the enabled datasets. */
  years: number[];
  year: number | null;
  onYearChange: (year: number) => void;
}

/** Describe a dataset's load state and item count for the selected year. */
function statusText(state: DatasetState | undefined, year: number | null) {
  if (!state) {
    return "";
  }
  if (state.status === "loading") {
    return "loading...";
  }
  if (state.status === "error") {
    return `error: ${state.error}`;
  }
  const count = state.items.filter((i) => i.year === year).length;
  return count > 0 ? `${count} items` : `no data for ${year}`;
}

/** Dataset toggles and year picker, plus a class legend for LULC. */
export function ControlPanel({
  enabled,
  onEnabledChange,
  opacity,
  onOpacityChange,
  states,
  years,
  year,
  onYearChange,
}: ControlPanelProps) {
  // List top-drawn datasets first.
  const ids = (Object.keys(DATASETS) as DatasetId[]).reverse();
  return (
    <SharedControlPanel
      title={
        <Flex align="center" gap="2">
          <a href="https://auspatious.com" target="_blank" rel="noreferrer">
            <img src="/favicon.svg" alt="Auspatious" width={60} height={66} />
          </a>
          Small Island developing States Land Use/Land Cover
        </Flex>
      }
    >
      <Text mb="3" color="gray.600">
        Tiles crossing the antimeridian are not shown.
      </Text>

      {/* Not a Field: Field shares one input id, which breaks multiple checkboxes. */}
      <Text fontSize="xs" color="gray.600" mb="1.5">
        Datasets
      </Text>
      <Stack gap="2" mb="3">
        {ids.map((id) => (
          <Box key={id}>
            <Checkbox.Root
              checked={enabled[id]}
              onCheckedChange={(d) => onEnabledChange(id, d.checked === true)}
            >
              <Checkbox.HiddenInput />
              <Checkbox.Control />
              <Checkbox.Label>
                {DATASETS[id].label}{" "}
                <Text as="span" color="gray.500">
                  {enabled[id] ? statusText(states[id], year) : ""}
                </Text>
              </Checkbox.Label>
            </Checkbox.Root>
            {enabled[id] ? (
              <Flex align="center" gap="3" pl="7" mt="1">
                <Text fontSize="xs" color="gray.600" whiteSpace="nowrap">
                  Opacity {Math.round(opacity[id] * 100)}%
                </Text>
                <Slider.Root
                  size="sm"
                  width="full"
                  min={0}
                  max={1}
                  step={0.01}
                  value={[opacity[id]]}
                  onValueChange={(d) => onOpacityChange(id, d.value[0] ?? 1)}
                  aria-label={[`${DATASETS[id].label} opacity`]}
                >
                  <Slider.Control>
                    <Slider.Track>
                      <Slider.Range />
                    </Slider.Track>
                    <Slider.Thumb index={0}>
                      <Slider.HiddenInput />
                    </Slider.Thumb>
                  </Slider.Control>
                </Slider.Root>
              </Flex>
            ) : null}
          </Box>
        ))}
      </Stack>

      <Field label="Year">
        <NativeSelect.Root>
          <NativeSelect.Field
            value={year ?? ""}
            onChange={(e) => onYearChange(Number(e.target.value))}
          >
            {years.map((y) => (
              <option key={y} value={y}>
                {y}
              </option>
            ))}
          </NativeSelect.Field>
          <NativeSelect.Indicator />
        </NativeSelect.Root>
      </Field>

      {enabled.lulc
        ? LULC_CLASSES.map(({ value, label, color }) => (
            <Flex key={value} align="center" gap="2">
              <Box w="3" h="3" bg={`rgb(${color.join(",")})`} />
              <Text fontSize="sm">{label}</Text>
            </Flex>
          ))
        : null}
    </SharedControlPanel>
  );
}
