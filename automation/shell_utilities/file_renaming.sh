#!/bin/bash
files=(
  "/home/glue_user/project_lf/ETL-TDD/claim_landing.py"
  "/home/glue_user/project_lf/ETL-TDD/claim_raw.py"
  "/home/glue_user/project_lf/ETL-TDD/claim_access.py"
  "/home/glue_user/project_lf/ETL-TDD/location_dim_optimised.py"
  "/home/glue_user/project_lf/ETL-TDD/policyholder_access.py"
  "/home/glue_user/project_lf/ETL-TDD/policyholder_landing.py"
  "/home/glue_user/project_lf/ETL-TDD/policyholder_raw.py"
  "/home/glue_user/project_lf/ETL-TDD/procedure_optimised.py"
  "/home/glue_user/project_lf/ETL-TDD/provider_access.py"
  "/home/glue_user/project_lf/ETL-TDD/provider_landing.py"
  "/home/glue_user/project_lf/ETL-TDD/provider_raw.py"
  "/home/glue_user/project_lf/ETL-TDD/date_dimension_optimised.py"
)

for file in "${files[@]}"
do
  filename=$(basename "$file")
  base=${filename%.*}
  ext=${filename#*.}
  IFS='_' read -ra ADDR <<< "$base"
  
  oldstage=${ADDR[1]}
  
  case $oldstage in
    landing)
      newstage=raw
      ;;
    raw)
      newstage=access
      ;;
    access)
      newstage=optimised
      ;;
    optimised)
      newstage=optimised
      ;;
  esac
  
  newname="/home/glue_user/project_lf/ETL-TDD/stage_${ADDR[0]}_into_${newstage}.${ext}"
  mv "$file" "$newname"
done
