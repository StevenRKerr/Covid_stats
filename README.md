# Coronavirus_stats

This repo consists of a project that amalgamates coronavirus data from various sources and then creates interactive visualisations for them.

The raw data can be found in the Data folder. The files are a mix of excel and csv, depending on where they came from.

The main code is corona.py. First it imports all the data, puts it in a suitable format for the purposes of this project and saves as pkl objects in the folder 'Pickle files'. Then it creates various interactive visualations using plotly express. Finally it saves those visualisations as HTML files in the folder "HTML files". They can then be embedded in webpages.

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
