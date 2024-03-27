import streamlit as st
import pandas as pd
from st_aggrid import GridOptionsBuilder, AgGrid
from st_aggrid.shared import JsCode
import base64

def ReadPictureFile(wch_fl):
    try:
        return base64.b64encode(open(wch_fl, 'rb').read()).decode()

    except:
        return ""

ShowImage = JsCode("""function (params) {
            var element = document.createElement("span");
            var imageElement = document.createElement("img");
        
            if (params.data.ImgPath != '') {
                imageElement.src = params.data.ImgPath;
                imageElement.width="20";
            } else { imageElement.src = ""; }
            element.appendChild(imageElement);
            element.appendChild(document.createTextNode(params.value));
            return element;
            }""")


df = pd.DataFrame({'Name': ['Iron Man', 'Walter White', 'Wonder Woman', 'Bat Woman'],
                   'Face': ['', '', '', ''],
                   'ImgLocation': ['Local', 'Local', 'Web', 'Web'],
                   'ImgPath': ['D:/ShawnP/icon-im.png', 
                               'D:/ShawnP/icon-ww.png', 
                               'https://i.pinimg.com/originals/ab/26/c3/ab26c351e435242658c3783710e78163.jpg',
                               'https://img00.deviantart.net/85f5/i/2012/039/a/3/batwoman_headshot_4_by_ricber-d4p1y86.jpg'] })

if df.shape[0] > 0:
    for i, row in df.iterrows():
        if row.ImgLocation == 'Local':
            imgExtn = row.ImgPath[-4:]
            row.ImgPath = f'data:image/{imgExtn};base64,' + ReadPictureFile(row.ImgPath)

gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_column('Face', cellRenderer=ShowImage)
gb.configure_column("ImgPath", hide = "True")

vgo = gb.build()
AgGrid(df, gridOptions=vgo, theme='blue', height=150, allow_unsafe_jscode=True )