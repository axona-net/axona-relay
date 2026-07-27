// smoke_kill_migration.mjs — a killed message must NOT resurrect when cached history
// MIGRATES to a fresh node that never saw the kill. Pre-4.9.2 the UP paths
// (PULLUP→REPLAYUP) and the graceful HANDOFF carried `msgs` only, so a behind/heir node
// adopted the killed body without its tombstone and would serve it to a late joiner
// (the kill-leak-via-migration class). Both paths must now carry tombstones (`dels`),
// applied BEFORE the bodies.
//
//   1. _onPullUp includes active tombstones in the REPLAYUP it sends up
//   2. _onReplayUp applies a migrated tombstone → the matching body is suppressed (no cache, no leak)
//   3. pubsubLeaveHandoff includes tombstones in the HANDOFF push
//   4. _onHandoff applies a migrated tombstone → heir does NOT resurrect the killed body
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
let n=0,fail=0; const ok=(m,c)=>{ if(c){console.log(`  ok ${++n} - ${m}`)}else{console.log(`  ✗  ${m}`);fail++} };
const REG=0x87n<<248n, idHex=(b)=>b.toString(16).padStart(66,'0');
const SELF=REG|0x011n, TOPIC=REG|0xabcn, HEIR=REG|0xab0n, PARENT=REG|0xab1n;

function mk({neighbors=[]}={}){
  const sends=[]; const clock={t:1_000_000};
  const dht={ getSelfId:()=>SELF, onRoutedMessage:()=>{}, routeMessage:(target,type,payload)=>sends.push({target,type,payload}),
    neighbors:()=>neighbors, bridgeId:()=>null, async findKClosest(){ return [idHex(HEIR)]; } };
  const am=new AxonaManager({dht, now:()=>clock.t}); am.nodeId=SELF;
  return {am,sends,clock};
}
// seed a root that holds m1 and has KILLED it (tombstone present, m1 spliced from cache)
function seedRootWithKill(am){
  const role=am._becomeRoot(TOPIC);
  role.cache.push({msgId:'m2',publishTs:300,json:'{}',seq:2,bytes:80}); role.cacheIds.add('m2');
  role.tombstones.set('m1',{exp:am._now()+3600_000, killTs:200, signer:'aa', seq:1});
  return role;
}

// 1+3. UP/HANDOFF sends carry the active tombstone
{
  const {am,sends}=mk();
  seedRootWithKill(am);
  // PULLUP from a behind parent
  am._onPullUp({topicId:idHex(TOPIC), sinceHw:0, parentId:idHex(PARENT)}, {targetId:SELF});
  const up=sends.find(s=>s.type==='pubsub:replayup');
  ok('PULLUP→REPLAYUP carries msgs', !!up && (up.payload.msgs||[]).some(m=>m.msgId==='m2'));
  ok('PULLUP→REPLAYUP carries the active tombstone (no leak on adopt)', !!up && (up.payload.dels||[]).some(d=>d.msgId==='m1'));
  // graceful HANDOFF
  sends.length=0;
  await am.pubsubLeaveHandoff();
  const ho=sends.find(s=>s.type==='pubsub:handoff');
  ok('HANDOFF carries the active tombstone too', !!ho && (ho.payload.dels||[]).some(d=>d.msgId==='m1'));
}

// 2. receiver of a REPLAYUP applies the tombstone → killed body is NOT cached
{
  const {am}=mk();
  const role=am._becomeRoot(TOPIC);   // fresh root, never saw the kill
  // an authored envelope for m1 the migration tries to bring up (author 'aa' = the killer)
  const env1=JSON.stringify({msgId:'m1', signerPubkey:'aa', message:'killed!'});
  await am._onReplayUp({topicId:idHex(TOPIC), dels:[{del:true,msgId:'m1',killTs:200,signer:'aa',seq:1,publishTs:200}],
                        msgs:[{msgId:'m1',publishTs:100,json:env1,seq:1}]}, {targetId:SELF});
  await am._ingestIdle();
  ok('migrated tombstone applied → killed body suppressed (not in cache)', !role.cacheIds.has('m1'));
  ok('migrated tombstone recorded', role.tombstones.has('m1'));
}

// 4. heir of a HANDOFF applies the tombstone → does NOT resurrect the killed body
{
  const {am}=mk();
  const env1=JSON.stringify({msgId:'m1', signerPubkey:'aa', message:'killed!'});
  await am._onHandoff({topicId:idHex(TOPIC), dels:[{del:true,msgId:'m1',killTs:200,signer:'aa',seq:1,publishTs:200}],
                       msgs:[{msgId:'m1',publishTs:100,json:env1,seq:1}]}, {targetId:SELF});
  const role=am.axonRoles.get(TOPIC);
  ok('heir adopts as root', !!role && role.isRoot===true);
  ok('heir does NOT resurrect the killed body', !role.cacheIds.has('m1'));
}

console.log(`\n${fail?'✗':'✓'} smoke_kill_migration: ${n} passed, ${fail} failed`);
process.exit(fail?1:0);
