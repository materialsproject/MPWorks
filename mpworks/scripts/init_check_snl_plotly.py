import plotly.plotly as py
import plotly.tools as tls
from plotly.graph_objs import *
stream_ids = tls.get_credentials_file()['stream_ids']
names = ['spacegroups', 'groupmembers', 'canonicals']

data = Data([
    Scatter(
        x=[], y=[], text=[],
        stream = Stream(token=stream_ids[i], maxpoints=100),
        mode='markers', name=name
    ) for i,name in enumerate(names)
])
layout = Layout(
    title='SNL group checks', xaxis=XAxis(title='SNL or SNL Group ID')
)
fig = Figure(data=data, layout=layout)
unique_url = py.plot(fig, filename='snl_group_check_stream')
