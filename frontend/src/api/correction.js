import{api,post}from'./client.js';export const listImports=()=>api('/imports');export const getImport=id=>api('/imports/'+id);export const createImport=data=>post('/imports',data);
