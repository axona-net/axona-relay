#!/usr/bin/env bash
# =============================================================================
# harness/probe-batch.sh — run N fresh-seed barrier-off probes back to back and
# summarize the residual strand-rate distribution + open-vs-owned bias. Each
# seed is a fresh topic map (roots form from scratch), barrier off (the
# representative config), churn-free. One summary line per seed in
# batch-summary.jsonl; strands classified by topic name (no topicId map needed).
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")/.."
SEEDS="${SEEDS:-810 811 812 813 814 815 816 817}"
DUR="${BATCH_DURATION_MS:-300000}"
OUT=harness/results/batch-summary.jsonl
: > "$OUT"

for seed in $SEEDS; do
  echo "── batch seed $seed  ($(date -u +%H:%M:%SZ))"
  PROBE_SEED="$seed" PROBE_READINESS_MS=0 PROBE_DURATION_MS="$DUR" ARM=batch bash harness/probe.sh >/dev/null 2>&1 || echo "  seed $seed: probe nonzero exit"
  node -e '
    const fs=require("fs"); const seed=process.argv[1];
    const ff=`harness/results/findings-${seed}.jsonl`, sf=`harness/results/summary-${seed}.json`;
    if(!fs.existsSync(sf)){ fs.appendFileSync("harness/results/batch-summary.jsonl", JSON.stringify({seed:+seed,err:"no summary"})+"\n"); process.exit(0); }
    let openS=0,ownedS=0;
    if(fs.existsSync(ff)) for(const l of fs.readFileSync(ff,"utf8").split("\n")){ if(!l.trim())continue; let x; try{x=JSON.parse(l)}catch{continue}
      if(x.detector==="stranded-write"){ x.topic.includes("owned")?ownedS++:openS++; } }
    const s=JSON.parse(fs.readFileSync(sf,"utf8"));
    const owed=(s.deliveredLive||0)+(s.deliveredLate||0)+(s.eventualReplay||0)+(s.missing||0);
    const row={ seed:+seed, ops:s.ops, owed, stranded:s.missing, strandedPct: owed?+((100*s.missing)/owed).toFixed(2):0,
      openStrand:openS, ownedStrand:ownedS, ownerUnresolved:s.ownerUnresolved, fullSetPct: s.ops?+((100*s.fullSetComplete)/s.ops).toFixed(1):0 };
    fs.appendFileSync("harness/results/batch-summary.jsonl", JSON.stringify(row)+"\n");
    console.log("  "+JSON.stringify(row));
  ' "$seed"
done

echo "── batch complete; distribution:"
node -e '
  const fs=require("fs");
  const rows=fs.readFileSync("harness/results/batch-summary.jsonl","utf8").trim().split("\n").map(l=>JSON.parse(l)).filter(r=>!r.err);
  const pcts=rows.map(r=>r.strandedPct).sort((a,b)=>a-b);
  const sum=(a)=>a.reduce((x,y)=>x+y,0);
  console.log("runs:",rows.length);
  console.log("strandedPct per seed:",JSON.stringify(rows.map(r=>({seed:r.seed,pct:r.strandedPct,open:r.openStrand,owned:r.ownedStrand}))));
  console.log("strandedPct min/median/max:",pcts[0],pcts[Math.floor(pcts.length/2)],pcts[pcts.length-1]);
  console.log("TOTAL strands open:",sum(rows.map(r=>r.openStrand)),"owned:",sum(rows.map(r=>r.ownedStrand)));
  console.log("runs with any strand:",rows.filter(r=>r.stranded>0).length,"/",rows.length);
'
