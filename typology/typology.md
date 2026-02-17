# Preliminary typology proposed for SIDS

The design of the LULC typology should balance model complexity, the feasibility of mapping using satellite data, and the need to capture key classes relevant to SIDS.

This preliminary typology is developed based on a review of existing land-cover products and the class definitions adopted by the UNCCD. 
Level-1 is chosen to align with the UNCCD, for simplicity and to facilitate training data collection. 
Level-2 classes are considered aspirational at this stage. The feasibility of mapping Level-2 classes will be assessed during the development phase, guided by stakeholder engagement, the relevance of classes to key degradation processes, and further analytical exploration. 

| Level-1 classes| Level-2 sub-classes | Rationale and Development Considerations |
|---|---|---|
| Tree Cover | Closed forest, Open forest | Forest classes can be subdivided by canopy cover (open vs. closed), phenology (evergreen vs. deciduous), and leaf type (broadleaved vs. needleleaved). Different LULC products have adopted different combinations of these subdivisions. Across SIDS, biome diversity is relatively limited, and temporal profiles are often less distinct due to persistent cloud cover and sparse time-series observations. Since we expect to rely more heavily on spectral characteristics than on multi-temporal signatures, crown density (open vs. closed forest) is the most feasible and robust Level-2 subdivision for SIDS. In the Pacific, subclasses of native evergreen forest and secondary forest are proposed. This separation is not always detectable using moderate-resolution remote sensing.  |
| Grassland | Shrubland, Grassland | Most global LULC products treat shrubland and grassland as top-level categories, but in practice, these classes are frequently confused, especially in heterogeneous dryland or coastal environments common across SIDS. Despite the classification challenges, distinguishing these classes remains potentially valuable for ecosystem management, erosion monitoring, and land degradation reporting. |
| Cropland | Rainfed, Irrigated| The rainfed/irrigated subdivision used by products such as GLC_FCS30D relies heavily on multi-temporal patterns. For many SIDS, persistent cloud cover, small field sizes, and limited temporal depth make it difficult to separate irrigation regimes reliably. Given the challenges of identifying cropland as a broad class, this subclass distinction will only be attempted if irrigation status is determined as particularly relevant for SIDS. This separation has not been proposed in the Pacific. Instead, subclasses that are more relevant for LDN and may be detectable from remote sensing include perennial tree crops and agroforestry. However, these systems can be easily confused with native tree cover. |
| Built-up | - | |
| Other | - | |
| Water | - | |
| Wetland | Herbaceous wetland, Mangrove | Products such as WorldCover separate herbaceous wetlands and mangroves into distinct classes. For SIDS, coastal ecosystems, especially mangroves, are of exceptionally high ecological and socio-economic importance, providing coastal protection, carbon storage, and nursery grounds for marine species. Given this importance and the relatively clear spectral separability of mangroves, maintaining mangroves as a dedicated Level-2 subclass is both feasible and highly beneficial for monitoring and conservation objectives. |



