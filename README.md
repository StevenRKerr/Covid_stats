# Covid_stats

This repo consists of a project that amalgamates coronavirus data from various sources and then creates interactive visualisations for them.

Some raw data is imported off the internet in the code. The rest of it can be found in the Data folder. The files are a mix of excel and csv, depending on where they came from.

importData.py imports all the data, puts it in a suitable format for the purposes of this project and saves as pkl objects in the folder 'Pickle files'. createGraphs.py creates various interactive visualations using plotly express, and saves those visualisations as HTML files in the folder "HTML files". They can then be embedded in webpages.

The data all pertains to the UK, and consists of the following quantities:
- Daily coronavirus deaths
- Daily new cases
- Daily hospital admissions for coronavirus
- Daily new tests
- Test positive rate
- Monthly GDP index, 2007-present
- Weekly universal credit claims, 2013-present
- Weekly deaths from all causes, 2010-present
- Deaths from influenza and pneumonia, 1902-present
- Leading causes of death, 2001-2018
- Government spending on economic stimulus packages in response to the coronavirus pandemic.
- NHS bed occupancy and availability.
- Unemployment
- Redundancies
- Claimaint count
- NHS and independent hospital bed occupancy/availability, by region.
