package com.neo4j.kettle.logging.util;

import org.pentaho.di.job.Job;
import org.pentaho.di.trans.Trans;
import org.pentaho.metastore.api.IMetaStore;

public class MetaStoreUtil {

  public static final IMetaStore findMetaStore( Job  job) {
    if (job.getJobMeta().getMetaStore()!=null) {
      return job.getJobMeta().getMetaStore();
    }
    if (job.getParentJob()!=null) {
      IMetaStore metaStore = findMetaStore( job.getParentJob() );
      if (metaStore!=null) {
        return metaStore;
      }
    }
    if (job.getParentTrans()!=null) {
      IMetaStore metaStore = findMetaStore( job.getParentTrans() );
      if (metaStore!=null) {
        return metaStore;
      }
    }

    return null;
  }

  public static final IMetaStore findMetaStore( Trans trans) {
    if (trans.getMetaStore()!=null) {
      return trans.getMetaStore();
    }
    if (trans.getTransMeta().getMetaStore()!=null) {
      return trans.getTransMeta().getMetaStore();
    }
    if (trans.getParentJob()!=null) {
      IMetaStore metaStore = findMetaStore( trans.getParentJob() );
      if (metaStore!=null) {
        return metaStore;
      }
    }
    if (trans.getParentTrans()!=null) {
      IMetaStore metaStore = findMetaStore( trans.getParentTrans() );
      if (metaStore!=null) {
        return metaStore;
      }
    }

    return null;
  }
}
