# Evaluate the impacts of GEF interventions on childrens' health conditions in Kenya

### Summary
The study presented here aims to quantify the association between GEF interventions and local health conditions of children under five years in Kenya, focusing on health measures including the prevalence of diarrhea and coughs. We test the hypothesis that improving environmental and socio-economic cobenefits of GEF project implementations may result in improved health outcomes. 

<p align="center">
  <img width="600" height="400" src="/viz_coughs.png">
</p>

Our study found localized associations in both variables tested, with a 17% reduction in the prevalence of coughs within 10 km from the intervention areas and a 9% reduction in the prevalence of diarrhea was found within a distance smaller than 3 km. Besides the direct measure of health outcomes, GEF interventions also demonstrated positive impacts on water accessibility, including the access to source water in dwelling and the presence of water at hand-washing facilities. Past literature has shown these variables to be risk factors for diarrhea and/or coughs. All the impacts above are stronger for clusters closer to GEF interventions. However, the estimated impacts on the health metrics were observed when the intermediate outcomes were controlled, meaning that GEF projects may also have influenced the metrics tested through other, still untested causal pathways.

<p align="center">
  <img width="500" height="350" src="/flowchart_gef.png">
</p>

Based on our study, we recommend the following steps to further understand the relationships between GEF interventions and the prevalence of diarrhea and coughing among children:
* Investigate GEF project components and identify other pathways through which GEF might impact the studied health outcomes
* Consider the health metrics that the GEF can impact - i.e., access to clean water and solid fuel use - when choosing intervention sites.  If two sites are equal in their potential environmental benefits, selecting sites with more vulnerable populations (i.e., those with poor access to water) may be appropriate.
* Perform the analysis with more precise distance measures (i.e., based on implementation GPS coordinates)
* Identify additional socioeconomic and health outcome data to enable a more detailed analysis of the distance decay of GEF impacts.
* Explore possible seasonal variances of GEF’s impacts


### Code
A notebook for exploratory data analysis can be found [here](https://github.com/JiayingChen0307/gef/blob/master/covariates.ipynb).

The Python script for combining and reforming data is linked [here](https://github.com/JiayingChen0307/gef/blob/master/propensity_prep.py).

Full report [here](https://github.com/JiayingChen0307/gef/blob/master/gef_report.pdf)
