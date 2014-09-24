import plotly.plotly as py
import plotly.tools as tls
from plotly.graph_objs import *
names = ['spacegroups', 'groupmembers', 'canonicals']

if __name__ == '__main__':
    stream_ids = tls.get_credentials_file()['stream_ids']
    xvals = range(1,101)
    zvals = [z[:] for z in [[0]*len(xvals)]*len(names)]
    data = Data([
        #Scatter(
        #    x=[], y=[], text=[],
        #    stream = Stream(token=stream_ids[i], maxpoints=300),
        #    mode='markers', name=name
        #) for i,name in enumerate(names),
        Heatmap(
            x=xvals, y=names, z=zvals,
            stream = Stream(token=stream_ids[0], maxpoints=300),
        )
    ])
    layout = Layout(
        title='SNL group checks',
        xaxis=XAxis(title='SNL or SNL Group ID')
    )
    fig = Figure(data=data, layout=layout)
    unique_url = py.plot(fig, filename='snl_group_check_stream')
