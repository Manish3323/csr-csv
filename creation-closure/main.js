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


Descriptions = ["SLM Calls to be skipped", "Bank Depd. Calls to be skipped", "CASHOUTERROR", "AB FULL/REJECT BIN OVERFILL", "ALL CASSETTES DOWN/FATAL", "CASHACCEPTORFAULTS", "JPERROR", "ENCRYPTORERROR", "CARDREADERERROR", "CLOSED", "INSUPERVISORY", "LOCAL/COMMUNICATIONERROR ", "EXCLUSIVELOCALERROR ","receipt printer  fatal","receipt paper out","receipt paper low"]
actionCodes = [27, 4, 15, 47, 26, 26, 46, 8, 8, 34, 7, 34,34,6,6,6]
statusCodes =["SLM", "Bank Depd.", "COB", "01570","00298", "01188", "01806", "02200", "00479", "00460", "02603", "00459", "00459","01700","01709","01770"]
GasperStatusDescription = ["SLM", "Bank Dependency", "Cash Out - Bank reason", "Reject Bin Overfill", "ALL Cassettes are Faulted", "Cash Acceptor Faulted Fatal Error", "JP : Not configured", "Encryptor: Error", "ATM Shutdown -Card reader faults", "ATM has been marked Down", "Mode switch moved to Supervisor", "ATM has been DISCONNECTED", "ATM has been DISCONNECTED","Recpt prntr:Fatal","Paper Out","recpt prntr: Paper low"]
data = {'ESQ/Inactive Problem Description': Descriptions, 'Action Code': actionCodes, 'Status Code': statusCodes, 'Gasper Status Description': GasperStatusDescription}
faultDist = pd.DataFrame(data)

fileInput1 = pn.widgets.FileInput(accept='.xlsx,.xls',)
fileInput2 = pn.widgets.FileInput(accept='.xlsx')
fileInput3 = pn.widgets.FileInput(accept='.xlsx')
uploadButton = pn.widgets.Button(name='Process Files', button_type = 'primary')

creationTable = pn.widgets.Tabulator(pagination='remote', page_size=5)
closure27Table = pn.widgets.Tabulator(pagination='remote', page_size=5)
closure4Table = pn.widgets.Tabulator(pagination='remote', page_size=5)
flmClosureTable = pn.widgets.Tabulator(pagination='remote', page_size=5)
datenow = datetime.now().strftime('%m/%d/%Y %H:%M')

creationFileName = f"creation-{datenow}.csv"
closure27FileName = f"AC-27-{datenow}.csv"
closure4FileName = f"AC-4-{datenow}.csv"
flmClosureFileName = f"FLM-{datenow}.csv"


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
closure27fileDownload = pn.widgets.FileDownload(filename=closure27FileName, callback=lambda: get_csv(closure27Table), button_type="success")
closure4fileDownload = pn.widgets.FileDownload(filename=closure4FileName, callback=lambda: get_csv(closure4Table), button_type="success")
flmClosureFileDownload = pn.widgets.FileDownload(filename=flmClosureFileName, callback=lambda: get_csv(flmClosureTable), button_type="success")

dataTables = ['closure27Table','closure4Table','flmClosureTable', 'creationTable']
downloadButtons = ['closure27fileDownload', 'closure4fileDownload','flmClosureFileDownload', 'creationFileDownload']
vanilla = pn.template.VanillaTemplate(title='CSV files')

def assignAtmId(df):
  return df['TERMINAL ID'][3:]

def assignAtmIdNew(df):
  return df['Terminal ID'][3:]

def assignStatusCode(row):
  faults = faultDist.index[faultDist['Action Code'] == row['Action Code Updated']]
  for each in faults:
    if(len(faultDist['ESQ/Inactive Problem Description'][each].strip()) > 0):
      return faultDist['Status Code'][each]
  return -999

def assignActionCode(row):
  faultsDesc = faultDist['ESQ/Inactive Problem Description'].values
  codes = []
  for i in range(len(faultsDesc)):
    if(faultsDesc[i].strip() == row['Fault'].strip()):
      codes.append(faultDist['Action Code'][i])
  return codes

def assign_new_col(row):
  row['ATM ID'] = row['ID']
  row['Status Code'] = row['STATUS NAME']
  return row
def assignAgeCol(row):
  date_str = row['Started at']
  date_format = '%d-%m-%Y %I:%M:%S %p'
  date_obj = datetime.strptime(date_str, date_format)
  diff = datetime.now() - date_obj
  days, seconds = diff.days, diff.seconds
  hours = days * 24 + seconds
  row['AGE'] = hours
  return row


def process_file(event):
    # pn.state.notifications.clear()
    if fileInput1.value is None:
      pn.state.notifications.error('Out of service file missing.', duration=3000)
      return
    if fileInput2.value is None:
      pn.state.notifications.error('CSR file missing.', duration=3000)
      return
    if fileInput3.value is None:
      pn.state.notifications.error('Inactive file missing.', duration=3000)
      return
    outOfService = pd.read_excel(BytesIO(fileInput1.value), skiprows=5)
    current = pd.read_excel(BytesIO(fileInput2.value),  usecols="A,G,H,I,L,J", skiprows=2)
    inactive = pd.read_excel(BytesIO(fileInput3.value))
    current = current.apply(take_action_code, axis=1)
    current = current.apply(assign_new_col, axis=1)
    outOfService = outOfService.apply(assignAgeCol, axis=1)
    outOfService = outOfService.sort_values(by='Started at', ascending=False)

    outOfService['ATM ID'] = outOfService.apply(assignAtmIdNew, axis=1)
    inactive['ATM ID'] = inactive.apply(assignAtmId, axis=1)

    
    csrOnly27And4 = current[(current['ACTION_CODE'] == 27) | (current['ACTION_CODE'] == 4)]
    csrWithout27And4 = current[(current['ACTION_CODE'] != 27) & (current['ACTION_CODE'] != 4)]
    outOfServiceOnly27and4 = outOfService.join(csrOnly27And4.set_index('ATM ID'), on='ATM ID', how="inner")
    outOfServiceWithout27and4 = outOfService[~outOfService['ATM ID'].isin(outOfServiceOnly27and4['ATM ID'])]
    outOfServiceWithout27and4['Action Code Updated'] = outOfServiceWithout27and4.apply(assignActionCode, axis=1)

    exploded = outOfServiceWithout27and4.explode('Action Code Updated')
    csrWithout27And4.rename(columns = {'ACTION_CODE':'Action Code Updated'}, inplace = True)

    mrged = pd.merge(exploded, csrWithout27And4, how='left', indicator='Exist')
    mrgedInactive = pd.merge(inactive, csrOnly27And4, how='left', on=['ATM ID'],  indicator='Exist')
    mrgedInactive.rename(columns = {'ACTION_CODE':'Action Code Updated'}, inplace = True)
    mrgedOutOfService = mrged[mrged['Exist']=='left_only']
    mrgedInactive = mrgedInactive[mrgedInactive['Exist']=='left_only']
    if(len(mrgedInactive) > 0):
      creationList = pd.concat([mrgedOutOfService,mrgedInactive])
    else:
      creationList = mrgedOutOfService

    print(creationList.columns)
    creationList['Created At'] = creationList['Started at']
    creationList['Status Code'] = creationList.apply(assignStatusCode, axis=1)

    creationList = creationList[['ATM ID', 'Action Code Updated', 'Status Code', 'Created At']]
    creationList.set_index('ATM ID', inplace=True)
    creationTable.value = creationList

    
    ## closure part
    outOfService['Action Code Updated For Closure'] = outOfService.apply(assignActionCode, axis=1)
    explodedOutOfService = outOfService.explode('Action Code Updated For Closure')
    print("here")
    closureListMergeOutOfService = pd.merge(current, explodedOutOfService, on=['ATM ID'], how="left", indicator='ExistIn')
    closureListOutOfService = closureListMergeOutOfService[closureListMergeOutOfService['ExistIn'] == 'left_only']
    print("here 2")
    closureListMergeInactive = pd.merge(closureListOutOfService, inactive, on=['ATM ID'], how="left", indicator='ExistForInactive')
    closureListInactive = closureListMergeInactive[closureListMergeInactive['ExistForInactive'] == 'left_only']
    closureList = closureListInactive
    closureList = closureList[['ATM ID', 'ACTION_CODE', 'Status Code', 'TICKET KEY']]
    closureList.set_index('TICKET KEY', inplace=True)
    closure27 = closureList[(closureList['ACTION_CODE'] == 27)]
    closure4 = closureList[(closureList['ACTION_CODE'] == 4)]
    closureNot27And4 = closureList[(closureList['ACTION_CODE'] != 4) & (closureList['ACTION_CODE'] != 27)]
    closureNot27And4 = closureNot27And4[(closureNot27And4['ACTION_CODE'] != 6) & (closureNot27And4['ACTION_CODE'] != 18)]
    closure27['Created At'] = datenow
    closure4['Created At'] = datenow
    closureNot27And4['Created At'] = datenow
    closure27Table.value = closure27
    closure4Table.value = closure4
    flmClosureTable.value = closureNot27And4

pd.options.mode.chained_assignment = None

pn.widgets.FileDownload(filename="creation.csv", callback=get_csv, button_type="success")

uploadButton.on_click(process_file)

def main():
  vanilla.main.append(
    pn.Row(
      pn.Column(
        "Out Of Service File",
        fileInput1,
      ),
      pn.Column(
        "CSR file",
        fileInput2,
      ),
      pn.Column(
        "Inactive file",
        fileInput3,
      ),
      pn.Row(
        uploadButton
      )
    )
  )
  vanilla.main.append(
      pn.Row(
        closure4Table,
        flmClosureTable,
      )
  )
  vanilla.main.append(
      pn.Row(
        creationTable,
        closure27Table,
      )
  )
  vanilla.main.append(
    pn.Column(
      creationFileDownload,
      closure27fileDownload,
      closure4fileDownload,
      flmClosureFileDownload
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