import { Box, chakra, Heading } from "@chakra-ui/react";
import type { ReactNode } from "react";
import { useState } from "react";
import { ExternalLink } from "./external-link.js";

export interface ControlPanelProps {
  /** Heading shown in the panel header. */
  title: ReactNode;
  /** Panel body content. */
  children: ReactNode;
}

/**
 * Floating, collapsible control panel in the top-left corner of the map.
 * Manages its own open/closed state. The body collapses; the
 * Auspatious footer stays visible.
 */
export function ControlPanel({
  title,
  children,
}: ControlPanelProps) {
  const [open, setOpen] = useState(true);

  return (
    <Box
      position="absolute"
      top="20px"
      left="20px"
      width="350px"
      maxHeight="calc(100% - 40px)"
      overflowY="auto"
      bg="white"
      color="gray.800"
      borderRadius="lg"
      boxShadow="0 2px 8px rgba(0, 0, 0, 0.1)"
      p="4"
      pointerEvents="auto"
      zIndex={1000}
    >
      <chakra.button
        type="button"
        onClick={() => setOpen((prev) => !prev)}
        display="flex"
        alignItems="center"
        justifyContent="space-between"
        width="100%"
        textAlign="left"
        cursor="pointer"
        userSelect="none"
        bg="transparent"
        border="none"
        p="0"
        m="0"
      >
        <Heading as="h2" size="md">
          {title}
        </Heading>
        <chakra.span
          fontSize="xs"
          transition="transform 0.2s"
          transform={open ? "rotate(0deg)" : "rotate(-90deg)"}
        >
          ▼
        </chakra.span>
      </chakra.button>
      {open ? (
        <Box mt="3" fontSize="sm">
          {children}
        </Box>
      ) : null}
      <Box
        mt="3"
        pt="2"
        borderTopWidth="1px"
        borderColor="gray.200"
        fontSize="xs"
        color="gray.600"
      >
        Data and app produced by{" "}
        <ExternalLink href="https://auspatious.com">Auspatious</ExternalLink>
      </Box>
    </Box>
  );
}
