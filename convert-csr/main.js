importScripts("https://cdn.jsdelivr.net/pyodide/v0.22.1/full/pyodide.js");

function sendPatch(patch, buffers, msg_id) {
  self.postMessage({
    type: 'patch',
    patch: patch,
    buffers: buffers
  })
}

async function startApplication() {
  console.log("Loading pyodide!");
  self.postMessage({type: 'status', msg: 'Loading pyodide'})
  self.pyodide = await loadPyodide();
  self.pyodide.globals.set("sendPatch", sendPatch);
  console.log("Loaded!");
  await self.pyodide.loadPackage("micropip");
  const env_spec = ['https://cdn.holoviz.org/panel/0.14.4/dist/wheels/bokeh-2.4.3-py3-none-any.whl', 'https://cdn.holoviz.org/panel/0.14.4/dist/wheels/panel-0.14.4-py3-none-any.whl', 'pyodide-http==0.1.0', 'openpyxl', 'pandas']
  for (const pkg of env_spec) {
    let pkg_name;
    if (pkg.endsWith('.whl')) {
      pkg_name = pkg.split('/').slice(-1)[0].split('-')[0]
    } else {
      pkg_name = pkg
    }
    self.postMessage({type: 'status', msg: `Installing ${pkg_name}`})
    try {
      await self.pyodide.runPythonAsync(`
        import micropip
        await micropip.install('${pkg}');
      `);
    } catch(e) {
      console.log(e)
      self.postMessage({
	type: 'status',
	msg: `Error while installing ${pkg_name}`
      });
    }
  }
  console.log("Packages loaded!");
  self.postMessage({type: 'status', msg: 'Executing code'})
  const code = `
  
import asyncio

from panel.io.pyodide import init_doc, write_doc

init_doc()

import asyncio
from concurrent.futures import ThreadPoolExecutor
import panel as pn
import pandas as pd
from datetime import datetime
from io import StringIO, BytesIO

pn.extension(notifications=True)
pd.options.mode.chained_assignment = None


fileInput1 = pn.widgets.FileInput(accept='.xlsx',)
uploadButton = pn.widgets.Button(name='Process Files', button_type = 'primary')

creationTable = pn.widgets.Tabulator(pagination='remote', page_size=5)

datenow = datetime.now().strftime('%m/%d/%Y %H:%M')

creationFileName = f"current-status-report-{datenow}.csv"



def take_action_code(row):
  row['ACTION_CODE']=row['ACTION_CODE'].split(' ')[0]
  row['ACTION_CODE']=int(row['ACTION_CODE'])
  return row

def get_csv(table):
    sio = StringIO()
    table.value.to_csv(sio)
    sio.seek(0)  # reset pointer
    return sio

creationFileDownload = pn.widgets.FileDownload(filename=creationFileName, callback=lambda: get_csv(creationTable), button_type="success")

dataTables = ['creationTable']
downloadButtons = ['creationFileDownload']
vanilla = pn.template.VanillaTemplate(title='CSV files')
def updateColValues(row):
  row['Action Code'] = int(row['Action Code'].split(' ')[0])
  row['Customer'] = "Oki - BNRMs for SBI"
  row['Site'] = "ONSITE"
  row['City'] = ""
  row['Status Description'] = row['Status Description'].ljust(100, ' ')
  return row
def process_file(event):
    # pn.state.notifications.clear()
    if fileInput1.value is None:
      pn.state.notifications.error('Out of service file missing.', duration=3000)
      return

    new_csr = pd.read_excel(BytesIO(fileInput1.value), skiprows=2)
    
    new_csr.rename(columns = {
      'ID':'ATM ID',
      'ADDRESS': 'Address',
      'START TIME':'Start Date Time',
      'ACTION_CODE': 'Action Code',
      'TICKET KEY': 'Ticket ID',
      'REFERENCE': 'Reference Number',
      'SHARED COMMENT': 'Comments',
      'STATUS NAME': 'Status Code',
      'STATUS DESCRIPTION':'Status Description'
    }, inplace = True)
    
    columns = ['Customer','ATM ID','Address','City','Site','Start Date Time','Action Code','Status Code','Status Description','Comments','Reference Number','Ticket ID']

    convert_csr = new_csr.apply(updateColValues, axis=1)

    convert_csr = convert_csr.filter(items=columns)
    convert_csr.set_index('Customer', inplace=True)
    creationTable.value = convert_csr


pd.options.mode.chained_assignment = None

pn.widgets.FileDownload(filename="creation.csv", callback=get_csv, button_type="success")

uploadButton.on_click(process_file)

def main():
  vanilla.main.append(
    pn.Row(
      pn.Column(
        "Current Status report (CSR file) new format",
        fileInput1,
      ),
      pn.Row(
        uploadButton
      )
    )
  )
  vanilla.main.append(
      pn.Row(
        creationTable,
      )
  )
  vanilla.main.append(
    pn.Column(
      creationFileDownload,
    )
  )

  vanilla.servable()
main()

await write_doc()
  `

  try {
    const [docs_json, render_items, root_ids] = await self.pyodide.runPythonAsync(code)
    self.postMessage({
      type: 'render',
      docs_json: docs_json,
      render_items: render_items,
      root_ids: root_ids
    })
  } catch(e) {
    const traceback = `${e}`
    const tblines = traceback.split('\n')
    self.postMessage({
      type: 'status',
      msg: tblines[tblines.length-2]
    });
    throw e
  }
}

self.onmessage = async (event) => {
  const msg = event.data
  if (msg.type === 'rendered') {
    self.pyodide.runPythonAsync(`
    from panel.io.state import state
    from panel.io.pyodide import _link_docs_worker

    _link_docs_worker(state.curdoc, sendPatch, setter='js')
    `)
  } else if (msg.type === 'patch') {
    self.pyodide.runPythonAsync(`
    import json

    state.curdoc.apply_json_patch(json.loads('${msg.patch}'), setter='js')
    `)
    self.postMessage({type: 'idle'})
  } else if (msg.type === 'location') {
    self.pyodide.runPythonAsync(`
    import json
    from panel.io.state import state
    from panel.util import edit_readonly
    if state.location:
        loc_data = json.loads("""${msg.location}""")
        with edit_readonly(state.location):
            state.location.param.update({
                k: v for k, v in loc_data.items() if k in state.location.param
            })
    `)
  }
}

startApplication()