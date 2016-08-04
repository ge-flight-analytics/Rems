uri_root <- 'https://fas.efoqa.com/api'
uris <- list(
  sys = list(
    auth = '/token'
  ),
  ems_sys = list(
    list = '/v2/ems-systems',
    ping = '/v2/ems-systems/%s/ping', 	# (ems-system_id)
    info = '/v2/ems-systems/%s/info'	# (ems-system_id)
  ),
  fleet = list(
    list = '/v2/ems-systems/%s/assets/fleets', 	#(ems-system_id)
    info = '/v2/ems-systems/%s/assets/fleets/%s'	#(ems-system_id, fleet_id)
  ),
  aircraft = list(
    list = '/v2/ems-systems/%s/assets/aircraft',	#(ems-system_id)
    info = '/v2/ems-systems/%s/assets/aircraft/%s'#(ems-system_id, aircraft_id)
  ),
  flt_phase = list(
    list = '/v2/ems-systems/%s/assets/flight-phases', 		#(ems-system_id)
    info = '/v2/ems-systems/%s/assets/flight-phases/%s' 	#(ems-system_id, flt_phase_id)
  ),
  airport = list(
    list = '/v2/ems-systems/%s/assets/airports',		#(ems-system_id)
    info = '/v2/ems-systems/%s/assets/airports/%s'		#(ems-system_id, airport_id)
  ),
  database = list(
    group = '/v2/ems-systems/%s/database-groups',			#(ems-system_id)
    field_group = '/v2/ems-systems/%s/databases/%s/field-groups',	#(ems-system_id, data_src_id)
    field = '/v2/ems-systems/%s/databases/%s/fields',
    query = '/v2/ems-systems/%s/databases/%s/query'
  ),
  analytic = list(
    search   = '/v2/ems-systems/%s/analytics',    # (emsSystemId)
    search_f = '/v2/ems-systems/%s/flights/%s/analytics', # (emsSystemId, flightId)
    group    = '/v2/ems-systems/%s/analytic-groups',    # (emsSystemId)
    group_f  = '/v2/ems-systems/%s/flights/%s/analytic-groups', # (emsSystemId, flightId)
    query    = '/v2/ems-systems/%s/flights/%s/analytics/query', # (emsSystemId, flightId)
    metadata = '/v2/ems-systems/%s/flights/%s/analytics/metadata' # (emsSystemId, flightId)
  )
)
