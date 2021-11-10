# Safe Location Density Transforms

This repo contains sample code that uses a Pandas plugin called `H3Pandas`, it allows for geo-location data to be
transformed into Polygons that conform to the [Hexagonal Hierarchical Spatial Index](https://eng.uber.com/h3/) aka H3.

This code allows you to take a Pandas DataFrame that contains coordinates and aggregate locations into various densities.

## Getting Started

```
$ virtualenv venv
$ source venv/bin/activate
$ pip install -U -r requirements.txt
```

Then launch Jupyter and checkout the example notebook.

```
$ jupyter notebook
```

