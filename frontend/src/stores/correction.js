import{listImports}from'../api/correction.js';export const imports={items:[],async load(){this.items=await listImports();return this.items}};
