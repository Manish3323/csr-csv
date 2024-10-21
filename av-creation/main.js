importScripts("https://cdn.jsdelivr.net/pyodide/v0.22.1/full/pyodide.js");

function sendPatch(patch, buffers, msg_id) {
  self.postMessage({
    type: "patch",
    patch: patch,
    buffers: buffers,
  });
}

async function startApplication() {
  console.log("Loading pyodide!");
  self.postMessage({ type: "status", msg: "Loading pyodide" });
  self.pyodide = await loadPyodide();
  self.pyodide.globals.set("sendPatch", sendPatch);
  console.log("Loaded!");
  await self.pyodide.loadPackage("micropip");
  const env_spec = [
    "ssl",
    "https://cdn.holoviz.org/panel/0.14.4/dist/wheels/bokeh-2.4.3-py3-none-any.whl",
    "https://cdn.holoviz.org/panel/0.14.4/dist/wheels/panel-0.14.4-py3-none-any.whl",
    "pyodide-http==0.2.1",
    "openpyxl",
    "pandas",
    "numpy",
    "xlrd"
  ];
  for (const pkg of env_spec) {
    let pkg_name;
    if (pkg.endsWith(".whl")) {
      pkg_name = pkg.split("/").slice(-1)[0].split("-")[0];
    } else {
      pkg_name = pkg;
    }
    self.postMessage({ type: "status", msg: `Installing ${pkg_name}` });
    console.log(pkg)
    try {
      await self.pyodide.runPythonAsync(`
        import micropip
        await micropip.install('${pkg}');
      `);
    } catch (e) {
      console.log(e);
      self.postMessage({
        type: "status",
        msg: `Error while installing ${pkg_name}`,
      });
    }
  }
  console.log("Packages loaded!");
  self.postMessage({ type: "status", msg: "Executing code" });
  const code = `
  
import asyncio

from panel.io.pyodide import init_doc, write_doc

init_doc()

import asyncio
from concurrent.futures import ThreadPoolExecutor
import panel as pn
import pandas as pd
import numpy as np
from datetime import datetime
from io import StringIO, BytesIO

pn.extension(notifications=True)
pd.options.mode.chained_assignment = None



Descriptions = ["CashOut_SBI","receipt printer  fatal","receipt paper out","receipt paper low","down - communication failure","Cash Acceptor Fatal(NCR)","magnetic card read/write  fatal","encryptor  fatal","CLOSE","supervisor mode alarm is on (NCR)","Reject bin overfill","All_CASSETTES_FATAL_SBI(NCR)","All_CASSETTES_FATAL_ADMIN_CASH(NCR)","cash handler  fatal","Close No Other Fault(NCR)"]
actionCodes = [15,6,6,6,34,26,8,8,34,7,47,26,31,26,34]
statusCodes = ["COB","01700","01709","01770","00459","01188","00479","02200","00460","02603","01570","00298","00294","00CDF","00460"]
GasperStatusDescription = [ "Cash Out - Bank reason", "Recpt prntr:Fatal", "Paper Out", "recpt prntr: Paper low", "ATM has been DISCONNECTED", "Cash Acceptor Faulted Fatal Error", "ATM Shutdown -Card reader faults", "Encryptor: Error", "Atm has been marked Down", "Mode switch moved to Supervisor", "Reject Bin Overfill", "ALL Cassettes are Faulted", "ALL Cassettes are Cash Out with Cash greater 25000", "Cash Handler Fatal-OBF", "Atm has been marked Down" ]
data = {'ESQ/Inactive Problem Description': Descriptions, 'Action Code': actionCodes, 'Status Code': statusCodes, 'Gasper Status Description': GasperStatusDescription}

faultDist = pd.DataFrame(data)

fileInput1 = pn.widgets.FileInput(accept='.xlsx,.xls',)
fileInput2 = pn.widgets.FileInput(accept='.xlsx,.xls')
fileInput3 = pn.widgets.FileInput(accept='.xlsx,.xls')
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
  row['ACTION_CODE']=int(row['ACTION_CODE_KEY'])
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

excludeFaults_S5 = [
    "All_CASSETTES_FATAL_ADMIN_CASH(NCR)", "cash handler  fatal"
]

excludeFaults_S1 = [
    "Cash Acceptor Fatal(NCR)"
]

excludeFaults = ['cassette  2  fatal','type 2 currency cassette low', 'cassette  3  fatal', 'type 3 currency cassette out', 'receipt printer  fatal', 'receipt paper out', 'COIN_DISPENSER_NOT_CONFIGURED (NCR)', 'cassette  1 not configured/Not Present', 'cassette  4 not configured/Not Present', 'type 1 currency cassette out', 'type 4 currency cassette out', 'receipt paper low']

dataTables = ['closure27Table','closure4Table','flmClosureTable', 'creationTable']
downloadButtons = ['closure27fileDownload', 'closure4fileDownload','flmClosureFileDownload', 'creationFileDownload']
vanilla = pn.template.VanillaTemplate(title='CSV files')

def assignAtmId(df):
  return df['TERMINAL ID'][3:]

def assignAtmIdNew(df):
  return df['Terminal ID'][3:]

def assignStatusCode(row):
  if(~np.isnan(row['Action Code Updated'])):
    found = faultDist[(row['Action Code Updated'] == faultDist['Action Code']) & (faultDist['ESQ/Inactive Problem Description'] == row['Fault'].strip())]
    if(len(found) > 0):
      return found['Status Code'].values[0]
  return -999

def assignActionCode(row):
  found = faultDist[faultDist['ESQ/Inactive Problem Description'] == row['Fault'].strip()]
  return found['Action Code'].values

def assign_new_col(row):
  row['ATM ID'] = row['ID'] if 'ID' in row else row['ATM_ID']
  row['Status Code'] = row['STATUS_CODE_KEY']
  return row
def assignAgeCol(row):
  date_str = row['Started at']
  date_format = '%d-%m-%Y %I:%M:%S %p'
  date_obj = datetime.strptime(date_str, date_format)
  row['AGE'] = int(row['Duration (minutes)'])
  return row

def stripFaults(row):
  row['Fault'] = row['Fault'].strip()
  return row
def assignDate(row):
  date_str = row['Created At']
  date_format = '%d-%m-%Y %I:%M:%S %p'
  if (pd.isnull(row['Created At'])):
    return date_str
  else : 
    date_obj = datetime.strptime(date_str, date_format)
    return date_obj.strftime('%m/%d/%Y')
def assignTime(row):
  date_str = row['Created At']
  date_format = '%d-%m-%Y %I:%M:%S %p'
  if (pd.isnull(row['Created At'])):
    return date_str 
  else : 
    date_obj = datetime.strptime(date_str, date_format)
    return date_obj.strftime('%H:%M:%S')

def update_faults(row):
    atm_id = row['ATM ID']
    
    # Exclude faults based on ATM ID prefix
    if atm_id.startswith('S5'):
        exclude_list = excludeFaults + excludeFaults_S5
    elif atm_id.startswith('S1'):
        exclude_list = excludeFaults + excludeFaults_S1
    else:
        exclude_list = excludeFaults
    
    # Filter out rows with fault descriptions in the exclude list
    if row['Fault'] in exclude_list:
        return None  # Exclude this row by returning None
    return row  # Keep the row otherwise    

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
    current = pd.read_excel(BytesIO(fileInput2.value),  usecols="A,F,G,H,I,L,J")
    inactive = pd.read_excel(BytesIO(fileInput3.value))
    current = current.apply(take_action_code, axis=1)
    current = current.apply(assign_new_col, axis=1)
    
    outOfService = outOfService.apply(stripFaults, axis=1)
    print(len(outOfService))
    outOfService = outOfService[~outOfService['Fault'].isin(excludeFaults)]
    print(len(outOfService))

    outOfService = outOfService.apply(assignAgeCol, axis=1)
    outOfService = outOfService.sort_values(by='Started at', ascending=False)

    outOfService['ATM ID'] = outOfService.apply(assignAtmIdNew, axis=1)
    outOfService = outOfService.apply(update_faults, axis=1)
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

    creationList['Status Code'] = creationList.apply(assignStatusCode, axis=1)
    creationList.rename(columns = {'Started at':'Created At'}, inplace = True)
    creationList['DATE'] = creationList.apply(assignDate, axis=1)
    creationList['TIME'] = creationList.apply(assignTime, axis=1)
    creationList.rename(columns = {'Fault':'HP fault'}, inplace = True)
    creationList = creationList[['ATM ID', 'Action Code Updated', 'Status Code', 'Created At','AGE','HP fault', 'DATE', 'TIME']]
    creationList = creationList[~creationList['HP fault'].isin(excludeFaults)]
    creationList.set_index('ATM ID', inplace=True)
    creationTable.value = creationList

    
    ## closure part
    outOfService['Action Code Updated For Closure'] = outOfService.apply(assignActionCode, axis=1)
    explodedOutOfService = outOfService.explode('Action Code Updated For Closure')
    closureListMergeOutOfService = pd.merge(current, explodedOutOfService, on=['ATM ID'], how="left", indicator='ExistIn')
    closureListOutOfService = closureListMergeOutOfService[closureListMergeOutOfService['ExistIn'] == 'left_only']
    closureListMergeInactive = pd.merge(closureListOutOfService, inactive, on=['ATM ID'], how="left", indicator='ExistForInactive')
    closureListInactive = closureListMergeInactive[closureListMergeInactive['ExistForInactive'] == 'left_only']
    closureList = closureListInactive
    closureList = closureList[['ATM ID', 'ACTION_CODE', 'Status Code', 'TICKET_KEY']]
    closureList.set_index('TICKET_KEY', inplace=True)
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
  `;

  try {
    const [docs_json, render_items, root_ids] =
      await self.pyodide.runPythonAsync(code);
    self.postMessage({
      type: "render",
      docs_json: docs_json,
      render_items: render_items,
      root_ids: root_ids,
    });
  } catch (e) {
    const traceback = `${e}`;
    const tblines = traceback.split("\n");
    self.postMessage({
      type: "status",
      msg: tblines[tblines.length - 2],
    });
    throw e;
  }
}

self.onmessage = async (event) => {
  const msg = event.data;
  if (msg.type === "rendered") {
    self.pyodide.runPythonAsync(`
    from panel.io.state import state
    from panel.io.pyodide import _link_docs_worker

    _link_docs_worker(state.curdoc, sendPatch, setter='js')
    `);
  } else if (msg.type === "patch") {
    self.pyodide.runPythonAsync(`
    import json

    state.curdoc.apply_json_patch(json.loads('${msg.patch}'), setter='js')
    `);
    self.postMessage({ type: "idle" });
  } else if (msg.type === "location") {
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
    `);
  }
};

startApplication();
