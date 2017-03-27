package com.github.pukkaone.odata.web.provider;

import java.util.Map;
import org.apache.olingo.commons.api.edm.provider.CsdlEdmProvider;

/**
 * Gets Entity Data Model provider.
 */
public interface CsdlEdmProviderFactory {

  /**
   * Gets Entity Data Model providers.
   *
   * @return map where key is service name and value is Entity Data Model provider
   */
  Map<String, CsdlEdmProvider> getEdmProviders();
}
