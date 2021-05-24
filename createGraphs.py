###################################################################### 

## Code author: Steven Kerr

## Description: 
# This code creates the graphs

###################################################################### 

import pandas as pd
import plotly.io as pio
import numpy as np
import plotly.express as px
import json
import importData as iD
import datetime
from datetime import date
import math

############################# IMPORT ################################


# ONS data is updated daily
# The file is downloaded automatically.
iD.importONS()

deaths = iD.Open('deaths') 

tests = iD.Open('tests')

cases = iD.Open('cases')

deathComp = iD.Open('deathComp')


# OWID data is updated daily
# The file is downloaded automatically.
iD.importOWID()

OWID = iD.Open('OWID')  


# The DailyHosp is updated daily.
# The file is downloaded automatically.
#iD.importDailyHosp()

newHospAdDiag = iD.Open('newHospAdDiag')

oldHospAdDiag = iD.Open('oldHospAdDiag')

dailyBedsOccCovid = iD.Open('dailyBedsOccCovid')

dailyMVbedsOccCovid = iD.Open('dailyMVbedsOccCovid')

# NHS pathways data is updated daily
#iD.importPathways()

pathways = iD.Open('pathways')

# The WeeklyHosp is updated daily.
# The file is downloaded automatically.
#iD.importWeeklyHosp()
weeklyGABedsOccCovid = iD.Open('weeklyGABedsOccCovid')

weeklyGABedsOccNonCovid = iD.Open('weeklyGABedsOccNonCovid')

weeklyBedsOccCovid = iD.Open('weeklyBedsOccCovid')

weeklyBedsOpen = iD.Open('weeklyBedsOpen')



# Flu and Covid surveilance is updated weekly
#iD.importSurveilance()
ICU = iD.Open('ICU')


# Mortality data is updated weekly, on Thursdays.
# The file is downloaded automatically.
iD.importMort()

Mort = iD.Open('Mort')

# importMonthlyHosp is updated around the 12th of each month
#iD.importMonthlyHosp()

newHospAd = iD.Open('newHospAd')

oldHospAd = iD.Open('oldHospAd')

monthlyBedsOcc = iD.Open('monthlyBedsOcc')

monthlyBedsOcc.name = 'monthlyBedsOcc'

monthlyBedsOccCovid = iD.Open('monthlyBedsOccCovid')

monthlyBedsOccCovid.name = 'monthlyBedsOccCovid'

monthlyMVbedsOcc = iD.Open('monthlyMVbedsOcc')

monthlyMVbedsOccCovid = iD.Open('monthlyMVbedsOccCovid')

admissionsByAge = iD.Open('admissionsByAge')


# Unempoyment data is updated roughly monthly
#iD.importUnemployment()
Unemployment = iD.Open('Unemployment')

# Redundancy data is updated roughly monthly
#iD.importRed()
redundancies =  iD.Open('Redundancies')

# JSA data is updated roughly monthly
#iD.importJSA()
JSA =  iD.Open('JSA')

# claimants data is updated roughly monthly
#iD.importClaimants()
claimants =  iD.Open('claimants')

# Government spending data is updated roughly monthly.
#iD.importGovSpending()
govSpending = iD.Open('govSpending')

# Deaths by age is updated weekly, on Tuesdays.
# The file needs to be downloaded manually.
#iD.importdeathByAge()
deathByAge = iD.Open('deathByAge') 

# UC data is updated sporadically.
# The file needs to be downloaded manually.
# iD.importUC()
UC = iD.Open('UC') 

# GDP data is updated monthly, approximately around the 10th of each month.
# The file needs to be downloaded manually.
#iD.importGDP()

GDP = iD.Open('yearlyGDP')

# IandP data is never updated.
#iD.importIandP()
IandP = iD.Open('IandP')

# LCD data is never updated.
#iD.importLCD()
LCD = iD.Open('LCD') 


# AvgBedsOcc never needs to be updated
# iD.importAvgBedsOcc()
# avgBedsOcc = iD.Open('avgBedsOcc')

#hisBedsOcc never needs to be updated

#iD.importHistBedsOcc()
histBedsOpen = iD.Open('histBedsOpen') 

histBedsOpen.name = 'histBedsOpen'

weeklyBedsOpen.name = 'weeklyBedsOpen'

weeklyBedsOpen[ weeklyBedsOpen == '-'  ] = np.nan


histBedsOcc = iD.Open('histBedsOcc') 


# retail is never updated
#
# retail = pd.read_csv('Data/Retail.csv')

################################# FORMATTING ############################

# Combine deaths, oldHospAd and newHospAd
hospAd = iD.stackData(oldHospAd, newHospAd, 'old')

hospAdDiag = iD.stackData(oldHospAdDiag, newHospAdDiag, 'old')

df = iD.mergeFrames(deaths, hospAd )

df = iD.mergeFrames(df, hospAdDiag )



# Combine cases with OWID
casesOWID = iD.mergeFrames(cases, OWID)

# Create yearly mort
iD.createYearlyMort(Mort, deaths)
yearlyMort = iD.Open('yearlyMort') 


# NHShosp is a list of NHS hospitals
NHShosp = list(histBedsOcc.columns)
    
NHShosp.remove('Date')
    
hospMeta = iD.Open('hospMeta')

# Stack historical and monthly bed occupancy data
weeklyBedsOcc = weeklyGABedsOccCovid
weeklyBedsOcc.iloc[:, 1:] = weeklyGABedsOccCovid.iloc[:, 1:] + weeklyGABedsOccNonCovid.iloc[:, 1:]



bedsOcc = iD.stackData(histBedsOcc, weeklyBedsOcc, 'new')

bedsOcc.name = 'bedsOcc'

bedsOccCovid = iD.stackData(weeklyBedsOccCovid, monthlyBedsOccCovid , 'old')

bedsOccCovid.name = 'bedsOccCovid'


# Stack the months and daily data for MV beds occupied Covid
MVbedsOccCovid = iD.stackData(monthlyMVbedsOccCovid, dailyMVbedsOccCovid, 'new')

# Combine MV beds occupied with MV beds occupied covid
MVbeds = pd.merge(monthlyMVbedsOcc, MVbedsOccCovid, how='outer'  )

# Add an MV beds occupied non-covid column
MVbeds['Mechanical ventilation beds occupied non-Covid-19 England'] =  MVbeds['Mechanical ventilation beds occupied England'] - MVbeds['Mechanical ventilation beds occupied Covid-19 England']



# totalCoronaDeaths is what it says.

totalCoronaDeaths2020 = deaths['Daily Covid-19 deaths UK'][deaths['Date'].dt.year == 2020 ].sum()

totalCoronaDeaths2021 = deaths['Daily Covid-19 deaths UK'][deaths['Date'].dt.year == 2021 ].sum()

# Add a column to IandP and LCD that is constant and equal to total 
# Covid-19 deaths. This is useful for plotting purposes
IandP.insert(1, 'Covid-19 deaths 2020 UK'  , totalCoronaDeaths2020  )

IandP.insert(1, 'Covid-19 deaths 2021 UK'  , totalCoronaDeaths2021  )

LCD.insert(1, 'Covid-19 deaths 2020 UK'  , totalCoronaDeaths2020  )

LCD.insert(1, 'Covid-19 deaths 2021 UK'  , totalCoronaDeaths2021  )



# This line is to make all the columns of IandP, LCD the same type, so they 
# can be  plotted together on the same plot by plotly.
IandP['Yearly influenza and pneumonia deaths England and Wales'] = IandP['Yearly influenza and pneumonia deaths England and Wales'].astype(np.int)

LCD.iloc[:, 1:] = LCD.iloc[:, 1:].astype(np.int)


# Calculate total Covid-19 excess deaths and total excess deaths this year.
totalNonCoronaED2020 = (yearlyMort['Weekly deaths UK 2020'] - yearlyMort['Mean weekly deaths 2015-2019<br>plus Covid-19 deaths UK 2020']).sum()

totalED2020 = (yearlyMort['Weekly deaths UK 2020'] - yearlyMort['Mean weekly deaths UK 2015-2019']).sum()

totalNonCoronaED2021 = (yearlyMort['Weekly deaths UK 2021'] - yearlyMort['Mean weekly deaths 2015-2019<br>plus Covid-19 deaths UK 2021']).sum()

totalED2021 = (yearlyMort['Weekly deaths UK 2021'] - yearlyMort['Mean weekly deaths UK 2015-2019']).sum()



totalTests = float(tests.loc[0, 'Cumulative tests UK'])

totalCases = float(cases['Daily new Covid-19 cases UK'].sum())

# Create a dictionary with some death related variables of interest.

jsonDict = { "TCD20": '{:,}'.format(totalCoronaDeaths2020), 
        "TNCED20": '{:,}'.format(round(totalNonCoronaED2020,2)),
        "TED20": '{:,}'.format(round(totalED2020,2)),
        "TCD21": '{:,}'.format(totalCoronaDeaths2021), 
        "TNCED21": '{:,}'.format(round(totalNonCoronaED2021,2)),
        "TED21": '{:,}'.format(round(totalED2021,2)),
        "tests": '{:,}'.format(math.trunc(totalTests)),
        "newpos": '{:,}'.format(math.trunc(totalCases))}

# Save that dictinary in json format. This can then be passed to other
# applications.

with open('jsonDict.json', 'w') as file:
    json.dump(jsonDict, file)


####################### FUNCTIONS ##################################

# Extract queries bedsOcc, monthlybedsOccCovid, and histBedsOpen
# according to area, NHS or private, and by year.

def extract(df, area, hosp, bed, year):
    
    colMarker = hospMeta.copy()

    if hosp=='NHS':
        
        colMarker = colMarker[ colMarker.index.isin(NHShosp) ]
        
        heading = 'NHS overnight '
    
    elif hosp == 'Non-NHS':
        
        colMarker = colMarker[ ~colMarker.index.isin(NHShosp) ]
        
        heading = 'Non-NHS overnight '
        
    else:
        
        heading = 'Hospital overnight '
      
    if bed == 'G&A':
        heading = heading + 'G&A beds '
    else:
        heading = heading + 'beds '
      
        
    if df.name == 'bedsOcc' or df.name == 'monthlyBedsOcc':
        heading = heading + 'occupied '
    elif df.name == 'bedsOccCovid' or df.name == 'monthlyBedsOccCovid':
        heading = heading + 'occupied Covid-19 '
    elif df.name ==  'weeklyBedsOpen':
        heading = heading + 'occupied + unoccupied '
    elif df.name == 'histBedsOpen':
        heading = heading + 'available '
     
    if area == 'England':
         heading = heading + 'England'
    else: 
         heading = heading + area
         colMarker = colMarker[colMarker == area ]
    
    heading = heading + ' ' + str(year)    
    
    cols = colMarker.index  
    
    output = df[ pd.DatetimeIndex(df['Date']).year == year ]
    
    cols = cols.intersection( output.columns )
    
    output[heading] = output[cols].sum(axis=1)
    
    output = output[ ['Date', heading ] ]
    
    output['Date'] = output['Date'].map(lambda x: x.replace(year=2020))
    
    if df.name == 'bedsOcc':
        output[heading][ output[heading] == 0  ] = np.nan
          
    return output

######################## CREATE FIGURES ###############################

# lastDate is the final data where there is data in the df data frame
lastDate =  str(df.iloc[-1,0])[:10]

# Deaths and hospital admissions figure
fig1 = px.line(df, x="Date", y=['Daily Covid-19 deaths UK', 'Daily hospital admissions with Covid-19 England', \
                    'Daily hospital admissions plus hospital diagnoses with Covid-19 England'] , \
               range_x=['2020-03-01',lastDate], \
             template = "simple_white", color_discrete_sequence =['red', 'gold', 'blue'] )

fig1.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=1.4,
    xanchor="left",
    x=0.02
)
    
)


# Tests figure
testsFig = px.bar(tests, x="Date", y=['Daily tests UK'], range_x=['2020-03-01',lastDate], \
             template = "simple_white", color_discrete_sequence =['magenta' ] )

testsFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="left",
    x=0.02
)
)



# Cases figure
casesFig =  px.bar(cases, x="Date", y=['Daily new Covid-19 cases UK'], \
             template = "simple_white" )
    
tests = px.line(tests, x='Date', y=['Daily tests UK'], color_discrete_sequence = ['orange'], template = "simple_white") 

casesFig.add_trace(tests.data[0])

casesFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="left",
    x=0.02
)
)


# Positive test rate figure 
testPosRateFig = px.line(OWID, x="Date", y=['Positive test rate UK'], range_x=['2020-03-01',lastDate], \
             template = "simple_white", color_discrete_sequence =['indigo' ] )

testPosRateFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="right",
    x=0.99
)
)

testPosRateFig.layout.yaxis.tickformat = ',.0%'

# Universal Credit figure
UCFig = px.line(UC, x="Date", y=['Weekly universal credit claims UK'], range_x=['2020-01-01', date.today()], \
             template = "simple_white", color_discrete_sequence =['deeppink' ], range_y=[0, 600000] )
    
    
Line0 = px.line(JSA, x='Date', y=['Number of JSA claimants'], range_x=['2020-01-01',date.today()], color_discrete_sequence = ['turquoise'], template = "simple_white") 

UCFig.add_trace(Line0.data[0])

UCFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="right",
    x=0.99
)
)



# GDP figure
GDPFig = px.line(GDP, x="Date", y=['Monthly GDP index UK 2020', 'Monthly GDP index UK 2019', \
              'Monthly GDP index UK 2018', 'Monthly GDP index UK 2017'], range_x=['2007-01-01','2007-12-01'], \
             template = "simple_white")

GDPFig.update_layout(xaxis=dict(tickformat="%b"),
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.5,
    xanchor="right",
    x=0.99
)
)



# Influenza and Pneumonia figure
IandPFig = px.line(IandP, x="Date", y=['Covid-19 deaths 2020 UK', 'Covid-19 deaths 2021 UK',\
        'Yearly influenza and pneumonia deaths England and Wales' ], \
             template = "simple_white", color_discrete_sequence =['orange' ,'teal', 'green' ] )

IandPFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="right",
    x=0.99
)
)



# yearly mortality figure
yearlyMortCols = ['Weekly deaths UK 2019',
                  'Weekly deaths UK 2020',
                  'Weekly deaths UK 2021',
                  'Mean weekly deaths UK 2015-2019',
                  'Mean weekly deaths 2015-2019<br>plus Covid-19 deaths UK 2020',
                  'Mean weekly deaths 2015-2019<br>plus Covid-19 deaths UK 2021']
                 
meanDeathsFig = px.line(yearlyMort, x="Date", y= yearlyMortCols, range_x=['2015-01-01','2015-12-24'], template = "simple_white" )

meanDeathsFig.update_layout(
    
)


meanDeathsFig.update_layout(xaxis=dict(tickformat="%d %b"),
            yaxis_title="Weekly deaths",
            legend_title="Variable:",
            legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="right",
            x=0.99) 
)



# Deaths by age group figure
deathByAgeFig = px.bar(deathByAge, x="Age group", y='Deaths' ,template = "simple_white")

deathByAgeFig.update_layout(
    yaxis_title="Deaths",
    legend_title="Variable:",
)



# Leading cause of death figure
LCDcols = list(LCD.columns)

LCDcols.remove('Date')

LCDFig = px.line(LCD, x="Date", y= LCDcols, template = "simple_white" ) 

LCDFig.update_layout(
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=1.25,
    xanchor="right",
    x=0.99
)
)


# Government spending figure
govSpendingFig = px.line(govSpending, x="Date", y=['Total value of CJRS and SEIS claims', \
        'Total value of approved business loans', 'Total NHS spending UK 2018-2019'],template = "simple_white" )

govSpendingFig.update_layout(
    yaxis_tickprefix = '£',
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.8,
    xanchor="left",
    x=0.02
)
)

# Regional beds occupied figures
regions = list(hospMeta.unique())

regions.append('England')

figNames = {'East of England': 'NHSBedsOccFigEoE',
            'London': 'NHSBedsOccFigL',
            'Midlands': 'NHSBedsOccFigM',
            'North East and Yorkshire': 'NHSBedsOccFigNE',
            'North West': 'NHSBedsOccFigNW',
            'South East': 'NHSBedsOccFigSE',
            'South West': 'NHSBedsOccFigSW',
            'England': 'NHSBedsOccFigE',
            }
            
 
reg = 'England'
          
def createRegOccFig(reg):
    frame = pd.merge(extract(bedsOcc, reg, 'NHS', 'G&A', 2021), extract(bedsOcc, reg, 'NHS', 'G&A', 2020), how='outer')
        
    frame = pd.merge(frame, extract(bedsOcc, reg, 'NHS', 'G&A', 2019), how='outer' )
             
    frame = pd.merge(frame, extract(bedsOcc, reg, 'NHS', 'G&A', 2018), how='outer' )
    
    frame = pd.merge(frame,  extract(bedsOcc, reg, 'NHS', 'G&A', 2017), how='outer' )
    
    frame['Mean NHS overnight G&A beds occupied ' + reg + ' 2017-2019'] = frame.iloc[:, 3:6].mean(axis=1)
    
    frame = frame.sort_values('Date')
    
    fig = px.line(frame, x='Date', y=frame.columns[1:],  \
                    template = "simple_white", 
                    color_discrete_sequence =[ 'orange', 'gold', 'red', 'green', 'blue', 'fuchsia'])
    
    frame = extract(bedsOccCovid, reg, 'NHS', '', 2021) 
        
    bar0 = px.bar(frame, x='Date', y=[frame.columns[1]],     \
                    color_discrete_sequence = ['purple'], template = "simple_white") 
    
    fig.add_trace(bar0.data[0])
            
    frame = extract(bedsOccCovid, reg, 'NHS', '', 2020) 
        
    bar1 = px.bar(frame, x='Date', y=[frame.columns[1]],     \
                    color_discrete_sequence = ['lightseagreen'], template = "simple_white") 
    
    fig.add_trace(bar1.data[0])
    
    fig.update_layout(
        xaxis=dict(tickformat="%b %d"),
        yaxis_title="",
        showlegend=True,
        legend_title="Variable:",
        legend=dict(
        yanchor="top",
        y=1.5,
        xanchor="left",
        x=0.1
    )
    )   
    
    return fig
    
for reg in regions:
 
    pio.write_html(createRegOccFig(reg), file='HTML files/Hospitals/' + figNames[reg] + '.html', auto_open=False) 


# Beds available figure
frame = pd.merge(extract(weeklyBedsOpen, 'England', 'NHS', 'G&A', 2021), extract(weeklyBedsOpen, 'England', 'NHS', 'G&A', 2020), how = 'outer')

frame = pd.merge(frame, extract(histBedsOpen, 'England', 'NHS', 'G&A', 2020), how = 'outer' )
                 
frame = pd.merge(frame, extract(histBedsOpen, 'England', 'NHS', 'G&A', 2019), how = 'outer' )

frame = pd.merge(frame, extract(histBedsOpen, 'England', 'NHS', 'G&A', 2018), how = 'outer'  )

frame = pd.merge(frame, extract(histBedsOpen, 'England', 'NHS', 'G&A', 2017), how = 'outer'  )

frame['Mean NHS overnight beds G&A available England 2017-2019'] = frame.iloc[:, 4:].mean(axis=1)

frame = frame.sort_values(by = ['Date'])

NHSBedsOpenFig = px.line(frame, x='Date', y=frame.columns[1:],  \
                template = "simple_white", 
                color_discrete_sequence =[ 'gold', 'red', 'green', 'blue', 'fuchsia', 'darkviolet', 'navy'])


    
NHSBedsOpenFig.update_layout(
    xaxis=dict(tickformat="%b %d"),
    yaxis_title="",
    showlegend=True,
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=1.35,
    xanchor="left",
    x=0.1
)
)    
    
    
# Private beds occupied figure
frame = pd.merge(extract(monthlyBedsOcc, 'England', 'Non-NHS', '', 2020),
                 extract(monthlyBedsOcc, 'England', 'Non-NHS', '', 2021), how='outer')

                 
privBedsOccFig = px.line(frame, x='Date', y=frame.columns[1:],  \
                template = "simple_white", 
                color_discrete_sequence =['green', 'gold'])
    
 
  
frame = pd.merge( extract(bedsOccCovid, 'England', 'Non-NHS', '', 2020),
                 extract(bedsOccCovid, 'England', 'Non-NHS', '', 2021), how='outer')
    
bar0 = px.bar(frame, x='Date', y=[frame.columns[1]],     \
                color_discrete_sequence = ['blue'], template = "simple_white") 

privBedsOccFig.add_trace(bar0.data[0])

bar1 = px.bar(frame, x='Date', y=[frame.columns[2]],     \
                color_discrete_sequence = ['fuchsia'], template = "simple_white") 

privBedsOccFig.add_trace(bar1.data[0])

    
privBedsOccFig.update_layout(
    xaxis=dict(tickformat="%b %d"),
    yaxis_title="",
    showlegend=True,
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=1.35,
    xanchor="left",
    x=0.1
)
)        
    

# Unemployment figure
unemploymentStart = datetime.datetime(2007, 1, 1)

unemploymentEnd = Unemployment.iloc[-1,0]

unemploymentFig = px.line(Unemployment, x="Date", y=['Unemployment rate (seasonally adjusted)'], \
                          range_x=[unemploymentStart, unemploymentEnd ], \
             template = "simple_white", color_discrete_sequence =[ 'yellowgreen'] )

unemploymentFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="right",
    x=0.99
)
)

unemploymentFig.layout.yaxis.tickformat = ',.1%'


# Mechanical ventilation beds figure
MVbedsFig = px.line(MVbeds,  x="Date",  y=['Mechanical ventilation beds occupied England' , \
                    'Mechanical ventilation beds occupied Covid-19 England',  \
              'Mechanical ventilation beds occupied non-Covid-19 England'],                            
                     range_x=['2020-04-02',lastDate], template = "simple_white" )


MVbedsFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="center",
    x=0.5
)
)    


# Death comparison figure
deathCompFig = px.line(deathComp,  x="Date",  y=['Weekly deaths within 28 days of a positive test' , \
        'Weekly deaths with Covid-19 on death certificate',] , color_discrete_sequence =[ 'lime', 'crimson'], \
                       template = "simple_white" )


deathCompFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=1.1,
    xanchor="right",
    x=0.99
)
)    



# Redundancies figure
redEnd = redundancies.iloc[-1,0]

redFig = px.line(redundancies, x="Date", y=['Redundancies in last 3 months'], range_x=['2007-01-01',str(date.today())], \
             template = "simple_white", color_discrete_sequence =['red' ] )

redFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="center",
    x=0.5
)
)

# Claimants figure
claimantsFig = px.line(claimants, x="Date", y=['Claimant count, seasonally adjusted'], \
             template = "simple_white", color_discrete_sequence =['goldenrod' ] )

claimantsFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="center",
    x=0.5
)
)

# Admissiong by age figure
admissionsByAgeFig = px.line(admissionsByAge, x='Date', y=admissionsByAge.columns,  \
                    template = "simple_white", 
                    color_discrete_sequence =[ 'blue', 'green', 'red', 'gold', 'fuchsia'])

admissionsByAgeFig.update_layout(
    yaxis_title="",
    showlegend=True,
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=1.35,
    xanchor="left",
    x=0.1
)
)            
    
  

# NHS pathways figure
pathwaysFig = px.line(pathways, x="Date", y=['Daily potential Covid-19 telephone triages England',
       'Daily potential Covid-19 online assessments England'], \
             template = "simple_white", color_discrete_sequence =['red', 'blue'] )

pathwaysFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=1,
    xanchor="center",
    x=0.5
)
    
)



# Surveilacne figure
surveilanceFig = px.line(ICU, x="Date", y=ICU.columns[1:], template = "simple_white" )

surveilanceFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=1,
    xanchor="left",
    x=0.02
)
    
)

 
# Vaccine fig

# Deaths and hospital admissions figure
vaccineFig = px.line(OWID, x="Date", y=['Individuals vaccinated', 'Individuals fully vaccinated'], \
             template = "simple_white", color_discrete_sequence =['gold', 'green'], \
                range_x=['2021-01-01', str(date.today())  ]   )

vaccineFig.update_layout(
    yaxis_title="",
    legend_title="Variable:",
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="left",
    x=0.02
)
)



#Create depression figure.
#This only needs to be done once - the figure is not dynamic

#depression = iD.Open('depression')


#depressionFig = px.bar(depression, x="Prevalence", y='Group', \
#                   color = 'Time period',   template = "simple_white", 
#                   barmode = 'group', orientation = 'h' )

#depressionFig.update_layout(
#   yaxis_title="",
#    legend_title="Variable:",
#    legend_traceorder= 'reversed',
#    legend=dict(
#    yanchor="top",
#    y=1.2,
#    xanchor="left",
#    x=0.02
#)
    
#)

#depressionFig.layout.xaxis.ticksuffix = '%'

#pio.write_html(depressionFig, file='HTML files/depressionFig.html', auto_open=True) 




# Create HTML files

pio.write_html(privBedsOccFig, file='HTML files/Hospitals/privBedsOccFig.html', auto_open=True)

pio.write_html(NHSBedsOpenFig, file='HTML files/Hospitals/NHSBedsOpenFig.html', auto_open=True)

pio.write_html(fig1, file='HTML files/fig1.html', auto_open=True)

pio.write_html(testsFig, file='HTML files/testsFig.html', auto_open=False)

pio.write_html(casesFig, file='HTML files/casesFig.html', auto_open=True)

pio.write_html(testPosRateFig, file='HTML files/testPosRateFig.html', auto_open=True)

pio.write_html(UCFig, file='HTML files/UCFig.html', auto_open=False)

pio.write_html(GDPFig, file='HTML files/GDPFig.html', auto_open=False)

pio.write_html(IandPFig, file='HTML files/IandPFig.html', auto_open=False)

pio.write_html(meanDeathsFig, file='HTML files/meanDeathsFig.html', auto_open=True)

pio.write_html(LCDFig, file='HTML files/LCDFig.html', auto_open=True)

pio.write_html(deathByAgeFig, file='HTML files/deathByAgeFig.html', auto_open=False)

pio.write_html(govSpendingFig, file='HTML files/govSpendingFig.html', auto_open=True)

pio.write_html(unemploymentFig, file='HTML files/unemploymentFig.html', auto_open=True)

pio.write_html(MVbedsFig, file='HTML files/MVbedsFig.html', auto_open=True)

pio.write_html(deathCompFig, file='HTML files/deathCompFig.html', auto_open=False)

pio.write_html(redFig, file='HTML files/redFig.html', auto_open=True)

pio.write_html(claimantsFig, file='HTML files/claimantsFig.html', auto_open=True)

pio.write_html(admissionsByAgeFig, file='HTML files/admissionsByAgeFig.html', auto_open=True)  

pio.write_html(pathwaysFig, file='HTML files/pathwaysFig.html', auto_open=True)  

pio.write_html(surveilanceFig, file='HTML files/surveilanceFig.html', auto_open=True) 

pio.write_html(vaccineFig, file='HTML files/vaccineFig.html', auto_open=True) 
