/**
 * keep track of a map of content id string to an array of 'promise resolvers'.
 * This can be useful e.g. to represent as a promise resolver potentially
 * many open connections for the same cid.
 * And once you have resolved the cid, iterate over the resolvers for the cid and resolve.reject them.
 */
export class MapCidToPromiseResolvers {
 constructor() {
   /**
    * @type {Map<string, Array<{
    *   resolve:(v: any) => void
    *   reject: (err?: Error) => void
    * }>>}
    */
   this.cidToResolvers = new Map
 }
 get size() {
   let count = 0;
   for (const [, value] of this.cidToResolvers.entries()) {
     count += value.length
   }
   return count
 }
 *[Symbol.iterator]() {
   for (const hangs of this.cidToResolvers.values()) {
     yield * hangs
   }
 }
 delete(cid) {
   this.cidToResolvers.delete(cid)
 }
 push(cid, { resolve, reject }) {
   const cleanup = () => {
     this.delete(cid)
   }
   const hangs = this.cidToResolvers.get(cid) ?? []
   const hang = {
     resolve(v) {
       cleanup()
       resolve(v)
     },
     reject(e) {
       cleanup()
       reject(e)
     },
   }
   this.cidToResolvers.set(cid, [...hangs, hang])
 }
}
