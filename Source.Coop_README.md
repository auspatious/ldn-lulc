This is a placeholder README for the Source.Coop data product.

# Small Island Developing States

Spatial extent: SIDSs plus some extra Pacific states/territories. See the list here:

Temporal extent: annual 2000-2025.

### Grids/regions
We used 2 regions because of the antimeridian.

1. Pacifc: EPSG:3832. 22 countries/territories. 517 grid tiles intersecting.
2. Non-Pacific. EPSG:6933. 38 countries/territories. 300 grid tiles intersecting.


### Method
LS7 buffer years
Cloud masking. Snow.

## Using this dataset:

```python
    search STAC-geoparquet
    load
    visualise
    etc.
```
