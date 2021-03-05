    SELECT CONCAT('/repositories/', ao.repo_id) as repository
      , CONCAT('/repositories/', ao.repo_id, '/resources/', ao.root_record_id) as resource
      , CONCAT('/repositories/', ao.repo_id, '/archival_objects/', ao.id) as archival_object_uri
      , replace(ao.title, '"', "'") as title
      , extent_data.extent_number_1
      , extent_data.extent_type_1
      , extent_data.extent_portion_1
      , extent_data.extent_summary_1 as extent_container_summary_1
      , extent_data.extent_number_2
      , extent_data.extent_type_2
      , extent_data.extent_portion_2
      , extent_data.extent_summary_2 as extent_container_summary_2
      , date_info.expressions as date_expression
      , date_info.begins as date_begin
      , date_info.ends as date_end
      , date_info.types as date_type
      , date_info.labels as date_label
      , scope_notes.notes as scope_content
      , NULL as use_standard_access_note
      , access_notes.notes as access_restrict
      , access_notes.machine_actionable_restriction_type as machine_actionable_restriction_type
      , access_notes.timebound_restriction_begin_date as timebound_restriction_begin_date
      , access_notes.timebound_restriction_end_date as timebound_restriction_end_date
      , process_notes.notes as process_info
      , otherfindaid_notes.notes as other_find_aid
      , arrangement_notes.notes as arrangement
    FROM archival_object ao
    LEFT JOIN enumeration_value ev on ev.id = ao.level_id
    JOIN resource on resource.id = ao.root_record_id
    LEFT JOIN (SELECT ao.id as ao_id
          , GROUP_CONCAT(DISTINCT IFNULL(date.expression, '') SEPARATOR '; ') as expressions
          , GROUP_CONCAT(DISTINCT IFNULL(date.begin, '') SEPARATOR '; ') as begins
          , GROUP_CONCAT(DISTINCT IFNULL(date.end, '') SEPARATOR '; ') as ends
          , GROUP_CONCAT(DISTINCT IFNULL(ev.value, '') SEPARATOR '; ') as types
          , GROUP_CONCAT(DISTINCT IFNULL(ev2.value, '') SEPARATOR '; ') as labels
          FROM date
          JOIN archival_object ao on ao.id = date.archival_object_id
          LEFT JOIN enumeration_value ev on ev.id = date.date_type_id
          LEFT JOIN enumeration_value ev2 on ev2.id = date.label_id
          WHERe ao.repo_id = 12
          GROUP BY ao.id) as date_info on date_info.ao_id = ao.id
    LEFT JOIN (SELECT note.archival_object_id
          , GROUP_CONCAT(DISTINCT JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content')) SEPARATOR '; ') as notes
			   FROM note
			   JOIN archival_object ao on ao.id = note.archival_object_id
			   WHERE ao.repo_id = 12
			   AND note.notes like '%scopecontent%'
         GROUP BY ao.id) as scope_notes on scope_notes.archival_object_id = ao.id
    LEFT JOIN (SELECT note.archival_object_id
          , GROUP_CONCAT(DISTINCT JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content')) SEPARATOR '; ') as notes
			   FROM note
			   JOIN archival_object ao on ao.id = note.archival_object_id
			   WHERE ao.repo_id = 12
			   AND note.notes like '%processinfo%'
         GROUP BY ao.id) as process_notes on process_notes.archival_object_id = ao.id
    LEFT JOIN (SELECT note.archival_object_id
          , GROUP_CONCAT(DISTINCT JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content')) SEPARATOR '; ') as notes
			   FROM note
			   JOIN archival_object ao on ao.id = note.archival_object_id
			   WHERE ao.repo_id = 12
			   AND note.notes like '%otherfindaid%'
         GROUP BY ao.id) as otherfindaid_notes on otherfindaid_notes.archival_object_id = ao.id
    LEFT JOIN (SELECT note.archival_object_id
          , GROUP_CONCAT(DISTINCT JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content')) SEPARATOR '; ') as notes
			   FROM note
			   JOIN archival_object ao on ao.id = note.archival_object_id
			   WHERE ao.repo_id = 12
			   AND note.notes like '%"type":"arrangement"%'
         GROUP BY ao.id) as arrangement_notes on arrangement_notes.archival_object_id = ao.id
	LEFT JOIN (SELECT note.archival_object_id
			   		, GROUP_CONCAT(JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content'))) as notes
			    	, GROUP_CONCAT(replace(replace(replace(JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.rights_restriction.local_access_restriction_type')), '[', ''), ']', ''), '"', '')) as machine_actionable_restriction_type
			    	, GROUP_CONCAT(JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.rights_restriction.begin'))) as timebound_restriction_begin_date
			    	, GROUP_CONCAT(JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.rights_restriction.end'))) as timebound_restriction_end_date
			   FROM note
			   JOIN archival_object ao on ao.id = note.archival_object_id
			   WHERE ao.repo_id = 12
		     AND note.notes like '%accessrestrict%'
         GROUP BY ao.id) as access_notes on access_notes.archival_object_id = ao.id
  LEFT JOIN (SELECT ao_id
            , SUBSTRING_INDEX(SUBSTRING_INDEX(extent_numbers, '; ', 1), '; ', -1) as extent_number_1
            , SUBSTRING_INDEX(SUBSTRING_INDEX(extent_types, '; ', 1), '; ', -1) as extent_type_1
            , SUBSTRING_INDEX(SUBSTRING_INDEX(extent_portions, '; ', 1), '; ', -1) as extent_portion_1
            , SUBSTRING_INDEX(SUBSTRING_INDEX(extent_summaries, '; ', 1), '; ', -1) as extent_summary_1
            , If(length(extent_numbers) - length(replace(extent_numbers, '; ', ''))>1, SUBSTRING_INDEX(SUBSTRING_INDEX(extent_numbers, '; ', 2), '; ', -1), NULL) as extent_number_2
            , If(length(extent_types) - length(replace(extent_types, '; ', ''))>1, SUBSTRING_INDEX(SUBSTRING_INDEX(extent_types, '; ', 2), '; ', -1) , NULL) as extent_type_2  
            , If(length(extent_portions) - length(replace(extent_portions, '; ', ''))>1, SUBSTRING_INDEX(SUBSTRING_INDEX(extent_portions, '; ', 2), '; ', -1), NULL) as extent_portion_2
            , If(length(extent_summaries) - length(replace(extent_summaries, '; ', ''))>1, SUBSTRING_INDEX(SUBSTRING_INDEX(extent_summaries, '; ', 2), '; ', -1), NULL) as extent_summary_2
          FROM 
          (SELECT ao.id as ao_id
            , GROUP_CONCAT(extent.number SEPARATOR '; ') as extent_numbers
            , GROUP_CONCAT(ev.value SEPARATOR '; ') as extent_types
            , GROUP_CONCAT(ev2.value SEPARATOR '; ') as extent_portions
            , GROUP_CONCAT(extent.container_summary SEPARATOR '; ') as extent_summaries
          FROM extent
          JOIN archival_object ao on ao.id = extent.archival_object_id
          LEFT JOIN enumeration_value ev on ev.id = extent.extent_type_id
          LEFT JOIN enumeration_value ev2 on ev2.id = extent.portion_id
          WHERE ao.repo_id = 12
          GROUP BY ao.id) as base_extent_table) as extent_data on extent_data.ao_id = ao.id
  WHERE resource.repo_id = 12
  AND resource.identifier like '%MS 343%'
  AND ev.value like '%file%'
  ORDER BY ao.parent_id, ao.position