# Situated Learning — Big Data Systems (CC ZG522)

**Student:** DEVATA SAI HARSHITH  
**BITS ID:** 2024DC04035  
**Repo:** [harshith-118/situatedlearningbds](https://github.com/harshith-118/situatedlearningbds)

AI-based benefits contract extraction pipeline designed on the Hadoop ecosystem (HDFS + Spark + Hive), with synthetic PoC data and Excel template population.

## Contents

| Path | Description |
|------|-------------|
| `DEVATA_SAI_HARSHITH_2024DC04035_Proposal.docx` | Project proposal |
| `DEVATA_SAI_HARSHITH_2024DC04035_Presentation.pptx` | 7-slide presentation |
| `poc/sample_data/` | Synthetic PDF/DOCX contracts, AI JSON, Excel template/export |
| `poc/scripts/map_json_to_excel.py` | Post-extraction Excel fill script |
| `poc/cluster/` | Lab cluster PoC: HDFS + Spark + Hive |
| `Situated Learning.docx` | Assignment brief |

## Lab PoC (3 Hadoop tools)

```bash
cd poc
chmod +x cluster/*.sh
./cluster/04_run_all.sh
```

See `poc/cluster/RUNBOOK.md` for steps and screenshot checklist.

## Note

All sample contracts are **synthetic**. No real client / LifeSource documents are included.
