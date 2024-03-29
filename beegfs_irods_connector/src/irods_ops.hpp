#ifndef IRODS_OPS_HPP
#define IRODS_OPS_HPP

#include "rodsType.h"
#include "inout_structs.h"
#include "config.hpp"
#include "rodsClient.h"

class beegfs_irods_connection {
 public:
   unsigned int thread_number;
   rcComm_t *irods_conn;
   //beegfs_irods_connection() : irods_conn(nullptr) {}
   explicit beegfs_irods_connection(unsigned int tnum) : thread_number(tnum), irods_conn(nullptr) {}
   ~beegfs_irods_connection(); 
   int send_change_map_to_irods(irodsBeegfsApiInp_t *inp) const;
   int populate_irods_resc_id(beegfs_irods_connector_cfg_t *config_struct_ptr);
   int instantiate_irods_connection(const beegfs_irods_connector_cfg_t *config_struct_ptr, int thread_number);
};

#endif
