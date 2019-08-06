#ifndef BEEGFS_CHANGE_TABLE_HPP
#define BEEGFS_CHANGE_TABLE_HPP

#include "inout_structs.h"

#include "config.hpp"
#include <string>
#include <ctime>
#include <vector>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/filesystem.hpp>

#include "change_table.capnp.h"


struct change_descriptor {
    unsigned long long            cr_index;
    std::string                   objectId;
    std::string                   parent_objectId;
    std::string                   object_name;
    std::string                   beegfs_path;     // the beegfs_path can be ascertained by the parent_fid and object_name
                                                   // however, if a parent is moved after calculating the beegfs_path, we 
                                                   // may have to look up the path using iRODS metadata
    ChangeDescriptor::EventTypeEnum last_event; 
    time_t                        timestamp;
    bool                          oper_complete;
    ChangeDescriptor::ObjectTypeEnum object_type;
    off_t                         file_size;
};

struct change_descriptor_seq_idx {};
struct change_descriptor_objectId_idx {};
struct change_descriptor_oper_complete_idx {};

typedef boost::multi_index::multi_index_container<
  change_descriptor,
  boost::multi_index::indexed_by<
    boost::multi_index::ordered_unique<
      boost::multi_index::tag<change_descriptor_seq_idx>,
      boost::multi_index::member<
        change_descriptor, unsigned long long, &change_descriptor::cr_index
      >
    >,
    boost::multi_index::hashed_unique<
      boost::multi_index::tag<change_descriptor_objectId_idx>,
      boost::multi_index::member<
        change_descriptor, std::string, &change_descriptor::objectId
      >
    >,
    boost::multi_index::hashed_non_unique<
      boost::multi_index::tag<change_descriptor_oper_complete_idx>,
      boost::multi_index::member<
        change_descriptor, bool, &change_descriptor::oper_complete
      >
    >

  >
> change_map_t;


// This is only to faciliate writing the objectId to the root directory 
int beegfs_write_objectId_to_root_dir(const std::string& beegfs_root_path, const std::string& objectId, change_map_t& change_map);

int beegfs_close(unsigned long long cr_index, const std::string& beegfs_root_path, const std::string& objectId, const std::string& parent_objectId,
                     const std::string& object_name, const std::string& beegfs_path, change_map_t& change_map);
int beegfs_mkdir(unsigned long long cr_index, const std::string& beegfs_root_path, const std::string& objectId, const std::string& parent_objectId,
                     const std::string& object_name, const std::string& beegfs_path, change_map_t& change_map);
int beegfs_rmdir(unsigned long long cr_index, const std::string& beegfs_root_path, const std::string& objectId, const std::string& parent_objectId,
                     const std::string& object_name, const std::string& beegfs_path, change_map_t& change_map);
int beegfs_unlink(unsigned long long cr_index, const std::string& beegfs_root_path, const std::string& objectId, const std::string& parent_objectId,
                     const std::string& object_name, const std::string& beegfs_path, change_map_t& change_map);
int beegfs_rename(unsigned long long cr_index, const std::string& beegfs_root_path, const std::string& objectId, const std::string& parent_objectId,
                     const std::string& object_name, const std::string& beegfs_path, const std::string& old_beegfs_path, 
                     change_map_t& change_map);
int beegfs_create(unsigned long long cr_index, const std::string& beegfs_root_path, const std::string& objectId, const std::string& parent_objectId,
                     const std::string& object_name, const std::string& beegfs_path, change_map_t& change_map);
int beegfs_mtime(unsigned long long cr_index, const std::string& beegfs_root_path, const std::string& objectId, const std::string& parent_objectId,
                     const std::string& object_name, const std::string& beegfs_path, change_map_t& change_map);
int beegfs_trunc(unsigned long long cr_index, const std::string& beegfs_root_path, const std::string& objectId, const std::string& parent_objectId,
                     const std::string& object_name, const std::string& beegfs_path, change_map_t& change_map);


int remove_objectId_from_table(const std::string& objectId, change_map_t& change_map);

size_t get_change_table_size(change_map_t& change_map);

void beegfs_print_change_table(const change_map_t& change_map);
bool entries_ready_to_process(change_map_t& change_map);
int serialize_change_map_to_sqlite(change_map_t& change_map, const std::string& db_file);
int deserialize_change_map_from_sqlite(change_map_t& change_map, const std::string& db_file);
int initiate_change_map_serialization_database(const std::string& db_file);
int set_update_status_in_capnproto_buf(unsigned char*& buf, size_t& buflen, const std::string& new_status);
int get_update_status_from_capnproto_buf(unsigned char* buf, size_t buflen, std::string& update_status);
void add_entries_back_to_change_table(change_map_t& change_map, std::shared_ptr<change_map_t>& removed_entries);
int add_capnproto_buffer_back_to_change_table(unsigned char* buf, size_t buflen, change_map_t& change_map, std::set<std::string>& current_active_objectId_list);
void remove_objectId_from_active_list(unsigned char* buf, size_t buflen, std::set<std::string>& current_active_objectId_list);
int write_change_table_to_capnproto_buf(const beegfs_irods_connector_cfg_t *config_struct_ptr, void*& buf, size_t& buflen,
                                          change_map_t& change_map, std::set<std::string>& current_active_objectId_list); 
int get_cr_index(unsigned long long& cr_index, const std::string& db_file);
int write_cr_index_to_sqlite(unsigned long long cr_index, const std::string& db_file);


#endif


