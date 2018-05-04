#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  9 04:58:28 2018

@author: user

https://bokeh.pydata.org/en/0.12.5/docs/reference/charts.html
"""

from DSJobsDB import Job
import DSJobsLDACalc as LDA

from datetime import datetime

from bokeh.plotting import figure, output_file, show
from bokeh.palettes import Category20#inferno#viridis#Spectral11

with Job() as db:
    NumberTopics_ = db.readJobTopic()
    Topics_ = db.getTopic()

x=[]
y=[]
  
a=0
while (a < len(Topics_)):
    b = 0
    xt=[]
    yt=[]
    while (b < len(NumberTopics_)):
        if (Topics_[a][0] == NumberTopics_[b][2]):
            date_ = datetime.strptime(NumberTopics_[b][0], '%Y/%m/%d')
            xt = xt + [date_]
            yt = yt + [NumberTopics_[b][3]]
        b += 1
    x.append(xt)
    y.append(yt)
    a += 1

# output to static HTML file
output_file("data/TopicsPerDay.html")

#mypalette=Category20()

# create a new plot with a title and axis labels
p = figure(title="Job Requirements per Date", x_axis_label='Date', y_axis_label='Number', width=1300, height=600, x_axis_type="datetime")

p.title.text = 'Click on legend entries to hide the corresponding lines'

# add a line renderer with legend and line thickness
a = 0
while (a < len(Topics_)):
    p.line(x[a], y[a], legend=(str(Topics_[a][1]+1) + ' | ' + Topics_[a][0]), line_width=2, line_color=Category20[20][a], line_dash='solid')
    a += 1
    
p.legend.location = "bottom_left"
p.legend.padding = 2
p.legend.spacing = 0
p.legend.label_text_font_size = "8pt"
p.legend.label_height = 15
p.legend.orientation = "vertical"

p.legend.click_policy="hide"

# show the results
#show(p)