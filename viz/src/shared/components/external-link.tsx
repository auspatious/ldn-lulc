import { Link } from "@chakra-ui/react";
import type { ReactNode } from "react";

export interface ExternalLinkProps {
  /** URL to open in a new tab. */
  href: string;
  children: ReactNode;
}

/** A link that opens in a new tab with `rel="noopener noreferrer"`. */
export function ExternalLink({ href, children }: ExternalLinkProps) {
  return (
    <Link
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      // Chakra's Link defaults to inline-flex, so a link that wraps becomes a
      // full-width box and pushes any punctuation after it onto the next line.
      display="inline"
      color="brand.600"
      textDecorationLine="underline"
      textUnderlineOffset="2px"
      _hover={{ color: "brand.700" }}
    >
      {children}
    </Link>
  );
}
