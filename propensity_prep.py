import pandas as pd
import numpy as np

covar = pd.read_csv('/Users/jiaying/Downloads/geolab/GEF/covariates_dhs.csv')
covar['crowded'] = covar['HV009'].astype('int')/covar['HV216']
dhs_filt = pd.read_csv('/Users/jiaying/Downloads/geolab/GEF/DHS_kenya/KE_2014_DHS/dhs_filt.csv')
covar = pd.merge(covar, dhs_filt,  how='right', left_on=['HV001','HV002'], right_on = ['V001','V002'])


def get_rate(dhs_filt,field,values,cluster_column='HV001'):
    df = dhs_filt[[cluster_column,field]]
    df = df.dropna(subset=[field])
    gb = df.groupby(cluster_column)
    cluster = list(dhs_filt[cluster_column].unique())
    rate_dict = {}
    for c in cluster:
        try:
            vals = gb.get_group(c)[field].tolist()
            vals = [v for v in vals if v != "Don't know"]
            count = 0
            for v in values:
                count += vals.count(v)
            rate = count/len(vals)
            rate_dict[c] = rate
        except:
            rate_dict[c] = np.nan

    return rate_dict

def get_rate_catag(dhs_filt,field,values=None,cluster_column='HV001'):
    df = dhs_filt[[cluster_column,field]]
    df = df.dropna(subset=[field])
    gb = df.groupby(cluster_column)
    cluster = list(dhs_filt[cluster_column].unique())
    rate_dict = {}
    values_dict = {}
    if field == 'HV215':
        values_dict['Tiles'] = 2
        values_dict['Thatch / grass / makuti'] = 1
        values_dict['Dung / mud / sod'] = 1
        values_dict['Iron sheets'] = 1
        values_dict['Tin cans'] = 1
        values_dict['Asbestos sheet'] = 1
        values_dict['Tin cans'] = 1
        values_dict['Concrete'] = 1
        values_dict['Other'] = 1
        values_dict['No roof'] = 0
    elif field == 'HV205':
        values_dict['Flush to piped sewer system'] = 2
        values_dict['Flush to septic tank'] = 2
        values_dict['Flush to pit latrine'] = 2
        values_dict['Flush to somewhere else'] = 2
        values_dict["Flush, don't know where"] = 2
        values_dict['Ventilated Improved Pit latrine (VIP)'] = 1
        values_dict['Pit latrine with slab'] = 1
        values_dict['Pit latrine without slab/open pit'] = 1
        values_dict['No facility/bush/field'] = 0
        values_dict['Composting toiletd'] = 0
        values_dict['Bucket toilet'] = 0
        values_dict['Hanging toilet/latrine'] = 0
    elif field == 'HV201':
        values_dict['Piped into dwelling'] = 2
        values_dict['Piped to yard/plot'] = 2
        values_dict['Public tap/standpipe'] = 2
        values_dict['Tube well or borehole'] = 1
        values_dict['Protected well'] = 1
        values_dict['Unprotected well'] = 1
        values_dict['River/dam/lake/ponds/stream/canal/irrigation channel'] = 0
        values_dict['Rainwater'] = 0
        values_dict['Tanker truck'] = 0
        values_dict['Cart with small tank'] = 0
        values_dict['Bottled water'] = 0
    elif field == 'V460':
        values_dict['All children'] = 2
        values_dict['Some children'] = 1
        values_dict['No'] = 0
        values_dict['No net in household'] = 0
    else:
        for i in range(len(values)):
            values_dict[values[i]] = i
    for c in cluster:
        try:
            vals = gb.get_group(c)[field].tolist()
            vals = [v for v in vals if v != 'Other']
            count = 0
            for v in vals:
                count += values_dict[v]
            rate = count/len(vals)
            rate_dict[c] = rate
        except:
            rate_dict[c] = np.nan

    return rate_dict

def recode():
    recode = {}
    recode['HV204']=get_rate(covar,'HV204',['0'])
    recode['HV237']=get_rate(covar,'HV237',['Yes'])
    recode['HV206']= get_rate(covar,'HV206',['Yes'])
    recode['HV230B'] = get_rate(covar,'HV230B',['Water is available'])
    recode['HV270'] = get_rate_catag(covar,'HV270',['Poorest','Poorer','Middle','Richer','Richest'])
    recode['HV215'] = get_rate_catag(covar,'HV215')
    recode['SH139A'] = get_rate(covar,'SH139A',['Yes'])
    recode['HV025'] = get_rate(covar,'HV025',['Urban'])
    recode['HV205'] = get_rate_catag(covar,'HV205')
    recode['HV201'] = get_rate_catag(covar,'HV201' )
    recode['V106'] = get_rate_catag(covar,'V106',["No education","Primary","Secondary","Higher"])
    recode['V460'] = get_rate_catag(covar,'V460')
    recode['V161'] = get_rate(covar,'V161',["Coal, lignite","Charcoal","Wood","Straw/shrubs/grass","Agricultural crop","Animal dung"])
    recode['S554F'] = get_rate(covar,'S554F',["Fast breathing"])
    recode['H31'] = get_rate(covar,'H31',["Yes, last 24 hours", "Yes, last two weeks"])
    recode['H31B'] = get_rate(covar,'H31B',["Yes"])
    recode['HV252']  = get_rate_catag(covar,'HV252',["Never","Less than monthly","Monthly","Weekly","Daily"])
    recode['HV242'] = get_rate(covar,'HV242',['Yes'])
    recode['crowded'] = covar.groupby('HV001')['crowded'].mean()

    d = {'HV001':list(recode['HV204'].keys())}
    covar_recode = pd.DataFrame(d)
    for i in recode.keys():
        try:
            covar_recode[i] = list(recode[i].values())
        except:
            covar_recode[i] = recode[i]

    covar_recode = covar_recode[covar_recode['HV001']!=423.0]

    diarrhea = get_rate(dhs_filt,'H11',['Yes, last two weeks','Yes, last 24 hours'],cluster_column='V001')
    covar_recode['diarrhea'] = list(diarrhea.values())
    covar_recode['HV001'] = covar_recode['HV001'].astype(int)

    #other covariates
    other = pd.read_csv('/Users/jiaying/Downloads/geolab/GEF/DHS_kenya/KE_2014_DHS_GPS/KEGC72FL/KEGC72FL.csv')
    other = other.drop(421)
    covar_recode['Enhanced_Vegetation_Index_2015'] = other['Enhanced_Vegetation_Index_2015']
    covar_recode['Annual_Precipitation_2015'] = other['Annual_Precipitation_2015']
    covar_recode['All_Population_Count_2015'] = other['All_Population_Count_2015']
    covar_recode['Nightlights_Composite'] = other['Nightlights_Composite']
    covar_recode['Mean_Temperature_2015'] = other['Mean_Temperature_2015']
    covar_recode['Aridity_2015'] = other['Aridity_2015']
    covar_recode['Malaria_Prevalence_2015'] = other['Malaria_Prevalence_2015']
    covar_recode['Gross_Cell_Production'] = other['Gross_Cell_Production']
    covar_recode['Travel_Times_2015'] = other['Travel_Times_2015']
    covar_recode['Proximity_to_Water'] = other['Proximity_to_Water']
    covar_recode['Land_Surface_Temperature_2015'] = other['Land_Surface_Temperature_2015']
    #replace missing values with NaN
    covar_recode = covar_recode.replace(-9999,np.nan)
    covar_recode = covar_recode.replace(float('inf'),np.nan)

    #explore distance correlation
    distance = pd.read_csv('/Users/jiaying/Downloads/geolab/GEF/survey_project_distance_final.csv')
    distance = distance[['cluster','distance']]
    distance = distance.rename(columns={'cluster':'HV001'})
    covar_recode = pd.merge(distance,covar_recode,how='left',on='HV001')
    #covar_recode = covar_recode.dropna()

    covar_recode.to_csv('/Users/jiaying/Downloads/geolab/GEF/propensity_input.csv',index=False)

if __name__ == '__main__':
    recode()
