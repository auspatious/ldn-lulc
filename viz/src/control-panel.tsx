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
import type { DatasetState, DatasetStates, SideConfig } from "./side.js";
import { DATASET_IDS, sideYears } from "./side.js";

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

interface SideControlsProps {
  /** Heading above the controls, used in compare mode. */
  title?: string;
  side: SideConfig;
  onChange: (side: SideConfig) => void;
  states: DatasetStates;
}

/** Dataset toggles with opacity, and a year picker, for one map side. */
function SideControls({ title, side, onChange, states }: SideControlsProps) {
  const { years, activeYear } = sideYears(side, states);
  // List top-drawn datasets first.
  const ids = [...DATASET_IDS].reverse();
  const setEnabled = (id: DatasetId, on: boolean) =>
    onChange({ ...side, enabled: { ...side.enabled, [id]: on } });
  const setOpacity = (id: DatasetId, value: number) =>
    onChange({ ...side, opacity: { ...side.opacity, [id]: value } });

  return (
    <Box mb="3">
      {title ? (
        <Text fontWeight="semibold" mb="2">
          {title}
        </Text>
      ) : null}
      {/* Not a Field: Field shares one input id, which breaks multiple checkboxes. */}
      <Text fontSize="xs" color="gray.600" mb="1.5">
        Datasets
      </Text>
      <Stack gap="2" mb="3">
        {ids.map((id) => (
          <Box key={id}>
            <Checkbox.Root
              checked={side.enabled[id]}
              onCheckedChange={(d) => setEnabled(id, d.checked === true)}
            >
              <Checkbox.HiddenInput />
              <Checkbox.Control />
              <Checkbox.Label>
                {DATASETS[id].label}{" "}
                <Text as="span" color="gray.500">
                  {side.enabled[id] ? statusText(states[id], activeYear) : ""}
                </Text>
              </Checkbox.Label>
            </Checkbox.Root>
            {side.enabled[id] ? (
              <Flex align="center" gap="3" pl="7" mt="1">
                <Text fontSize="xs" color="gray.600" whiteSpace="nowrap">
                  Opacity {Math.round(side.opacity[id] * 100)}%
                </Text>
                <Slider.Root
                  size="sm"
                  width="full"
                  min={0}
                  max={1}
                  step={0.01}
                  value={[side.opacity[id]]}
                  onValueChange={(d) => setOpacity(id, d.value[0] ?? 1)}
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
            value={activeYear ?? ""}
            onChange={(e) => onChange({ ...side, year: Number(e.target.value) })}
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
    </Box>
  );
}

export interface ControlPanelProps {
  /** The main map, or the left side in compare mode. */
  left: SideConfig;
  onLeftChange: (side: SideConfig) => void;
  /** The right side in compare mode. */
  right: SideConfig;
  onRightChange: (side: SideConfig) => void;
  compare: boolean;
  onCompareChange: (compare: boolean) => void;
  states: DatasetStates;
}

/** Layer controls per side, a compare toggle, and the LULC legend. */
export function ControlPanel({
  left,
  onLeftChange,
  right,
  onRightChange,
  compare,
  onCompareChange,
  states,
}: ControlPanelProps) {
  const showLegend = left.enabled.lulc || (compare && right.enabled.lulc);
  return (
    <SharedControlPanel
      title={
        <Flex align="center" gap="2">
          <a href="https://auspatious.com" target="_blank" rel="noreferrer">
            <img
              src={`${import.meta.env.BASE_URL}favicon.svg`}
              alt="Auspatious"
              width={60}
              height={66}
            />
          </a>
          Small Island developing States Land Use/Land Cover
        </Flex>
      }
    >
      <Checkbox.Root
        mb="3"
        checked={compare}
        onCheckedChange={(d) => onCompareChange(d.checked === true)}
      >
        <Checkbox.HiddenInput />
        <Checkbox.Control />
        <Checkbox.Label>Compare (swipe)</Checkbox.Label>
      </Checkbox.Root>

      <SideControls
        title={compare ? "Left" : undefined}
        side={left}
        onChange={onLeftChange}
        states={states}
      />
      {compare ? (
        <SideControls
          title="Right"
          side={right}
          onChange={onRightChange}
          states={states}
        />
      ) : null}

      {showLegend
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
