# Land Use Land Cover Typology for SIDS

This document defines the Land Use/Land Cover (LULC) typology for a 30 m LULC product for Small Island Developing States (SIDS), developed to support the Land Degradation Neutrality (LDN) initiative (CI GEF project 11834). Annual maps will be generated for 2000-2025 from Landsat geometric median and median absolute deviation composites.

The typology is a simple hierarchy: a consistent set of Level-1 classes for LDN reporting, plus a small number of Level-2 subclasses where they are feasible to map and locally relevant. Level-2 classes aggregate to Level-1.

## Design requirements

The typology should:

- support calculation of the LDN indicators
- enable assessment of the spatial extent and key drivers of land degradation
- capture country-specific land degradation processes, as emphasised in UNCCD guidance
- be feasible to map using remotely sensed data
- be mutually exclusive and collectively exhaustive, so every valid pixel gets one and only one class
- balance classification complexity with the suitability and availability of input data
- be compatible, where practicable, with classification systems already used by relevant countries and existing data-collection efforts

Level-1 follows the UNCCD default seven-class typology (adapted from the IPCC land-use categories), so data can serve multiple reporting purposes and IPCC land-use change factors can be applied to soil organic carbon estimates. Definitions are adapted to be practical for satellite-based mapping.

## Level-1 classes

| Level-1 Class | Proposed Definition | Rationale |
| ------------- | ------------------- | --------- |
| Tree cover | Areas dominated by trees, typically forming a continuous or semi-continuous canopy. Includes natural forests, plantations and tree crops where tree cover is dominant, but excludes shrub-dominated vegetation and mangroves. | Broadly consistent with the IPCC Forest Land category but does not enforce thresholds for canopy cover, tree height or minimum area, as these cannot all be determined consistently from Landsat alone. Consistent with ESA WorldCover in distinguishing tree cover from shrubland and mangroves. |
| Grassland | Areas dominated by herbaceous vegetation or shrubs that do not meet the Tree cover definition. Includes natural grasslands, rangelands, pastures and shrubland. | Broadly consistent with the IPCC and UNCCD Grassland category, which includes shrub-dominated land not classified as Forest Land, but adapted to focus on vegetation dominance rather than land use. Corresponds broadly to the combined ESA WorldCover Grassland and Shrubland classes. |
| Cropland | Areas used for cultivation of crops, typically showing seasonal vegetation dynamics associated with planting and harvesting cycles. Includes annual crops and herbaceous cropping systems. | Aligned with IPCC cropland but excludes agroforestry systems where woody vegetation is the dominant cover, which are classified as Tree cover. Relies on seasonal vegetation dynamics rather than land management information, which is not directly observable. Broadly consistent with the ESA WorldCover cropland class. |
| Built-up | Areas dominated by artificial surfaces such as buildings, roads and infrastructure. May include small patches of vegetation within built environments, but excludes large urban green areas that are predominantly vegetated. | Consistent with IPCC settlements but simplified for remote sensing detection of impervious surfaces. Closely aligned with the WorldCover built-up class. Excluding large urban green areas keeps it consistent with the vegetation-based classes and improves separability in Landsat data. |
| Water | Areas covered by surface water for most of the year, such as lakes, rivers and streams, artificial reservoirs, coastal lagoons and estuaries. | Consistent with the UNCCD water bodies definition and the WorldCover water class. |
| Wetland | Areas where vegetation (woody or herbaceous) is regularly or permanently inundated or saturated with water. Includes mangroves, marshes and swamps, but excludes open water bodies. | Broadly consistent with IPCC wetlands (excluding open water) and UNCCD definitions. Harmonised with the WorldCover wetland and mangrove classes by emphasising vegetation under hydrological influence. |
| Other | Areas with little or no vegetation cover, including bare soil, sand, exposed rock and ice. | Aligned with IPCC "other land" but narrowed to non-vegetated natural surfaces detectable via remote sensing. |

## Level-2 classes

A minimal hierarchy is proposed, with Shrubland and Mangrove as the only Level-2 classes. Both are particularly relevant to land degradation assessment in Pacific SIDS and are represented in existing Pacific field-data collection.

| Level-1 Class | Level-2 Class | Proposed Definition |
| ------------- | ------------- | ------------------- |
| Grassland | Shrubland | Areas dominated by shrubs or other low woody vegetation, where trees are not the dominant cover. |
| Wetland | Mangrove | Intertidal wetlands dominated by mangrove vegetation. |

Shrubland is mapped as a distinct Level-2 class, consistent with ESA WorldCover, and aggregated with herbaceous Grassland to derive Level-1 Grassland. Mangrove is mapped separately and aggregated with other vegetated wetlands to derive Level-1 Wetland.

**Status: provisional.** Inclusion of Shrubland and Mangrove in the final product depends on stakeholder confirmation of their decision relevance and on pilot testing showing they can be interpreted and mapped consistently. Additional subclasses, including tree crops, may be considered later where there is a demonstrated decision need and adequate reference data.

## Implementation rules

These rules guide both map production and validation-data collection.

1. **Dominant observable cover.** Label each pixel by its dominant observable land cover during the reference period. Where multiple covers occur, assign the class with the largest estimated cover fraction. Dominance does not require more than 50% cover. This is needed because the product has no mixed-pixel labels.
2. **Hydrologically influenced areas.** Open water is Water. Areas dominated by vegetation under regular or persistent hydrological influence are Wetland. Mangroves are always Wetland.
3. **Cultivated vegetation.** Annual and herbaceous cropping systems are Cropland. Tree crops and agroforestry are classified by dominant observable cover, so areas dominated by trees are Tree cover.
4. **Modified non-vegetated surfaces.** Areas dominated by buildings, roads or other constructed surfaces are Built-up. Extraction areas, stockpiles, waste sites and cleared ground are classified by dominant observable cover, generally Other unless constructed surfaces dominate.
5. **Temporary conditions.** Temporary flooding, burning, exposed soil or vegetation loss should not automatically determine the annual class. Classification should reflect dominant or persistent cover during the reference period, using multi-date observations where possible.
6. **Uncertain reference labels.** Apply the same rules when collecting validation data. Interpreters should record label confidence and, where useful, secondary cover or estimated cover fractions. Samples that cannot be labelled reliably are flagged for review or excluded per the validation protocol.

These rules will be tested through mapping and validation. Recurring ambiguities may require more detailed class-specific decision rules or targeted use of ancillary and higher-resolution reference data.

## Training labels and known classification issues

Initial training data will come from agreement among ESA CCI Land Cover, ESA WorldCover and Impact Observatory (IO) LULC, reclassified as below.

| Level-1 Class | ESA WorldCover classes | IO LULC classes |
| ------------- | ---------------------- | --------------- |
| Tree cover | Tree cover | Trees |
| Grassland | Shrubland, Grassland | Rangeland |
| Cropland | Cropland | Crops |
| Built-up | Built-up | Built Area |
| Other | Bare/sparse vegetation, Snow and Ice, Moss and lichen | Bare Ground, Snow/Ice |
| Water | Permanent water bodies | Water |
| Wetland | Herbaceous wetland, Mangroves | Flooded Veg. |

These products differ in resolution, reference year, class definitions, thresholds and methods, so the class definitions cannot be applied perfectly to every derived label, and some ambiguity will propagate into the maps. Validation data must apply the project definitions independently of the training labels, so the accuracy assessment captures errors from ambiguous training labels rather than reproducing them.
