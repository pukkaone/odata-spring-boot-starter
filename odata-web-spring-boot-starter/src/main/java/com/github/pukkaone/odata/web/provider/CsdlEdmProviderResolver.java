package com.github.pukkaone.odata.web.provider;

import org.apache.olingo.commons.api.edm.provider.CsdlEdmProvider;

/**
 * Gets Entity Data Model provider.
 */
public interface CsdlEdmProviderResolver {

  /**
   * Resolves service name to Entity Data Model provider.
   *
   * @param serviceName
   *     service name
   * @return Entity Data Model provider, or null if not found
   */
  CsdlEdmProvider findByServiceName(String serviceName);
}
