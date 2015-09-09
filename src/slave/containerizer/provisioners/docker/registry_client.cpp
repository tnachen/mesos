/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <vector>

#include <process/defer.hpp>
#include <process/dispatch.hpp>

#include "slave/containerizer/provisioners/docker/registry_client.hpp"
#include "slave/containerizer/provisioners/docker/token_manager.hpp"

using std::string;
using std::vector;

using process::Failure;
using process::Future;
using process::Owned;
using process::Process;

using process::http::Request;
using process::http::Response;
using process::http::URL;

namespace mesos {
namespace internal {
namespace slave {
namespace docker {
namespace registry {

using FileSystemLayerInfo = RegistryClient::FileSystemLayerInfo;

using ManifestResponse = RegistryClient::ManifestResponse;

const Duration RegistryClient::DEFAULT_MANIFEST_TIMEOUT_SECS = Seconds(10);

const size_t RegistryClient::DEFAULT_MANIFEST_MAXSIZE_BYTES = 4096;

static const uint16_t DEFAULT_SSL_PORT = 443;

class RegistryClientProcess : public Process<RegistryClientProcess>
{
public:
  static Try<Owned<RegistryClientProcess>> create(
      const URL& authServer,
      const URL& registry,
      const Option<RegistryClient::Credentials>& creds);

  Future<RegistryClient::ManifestResponse> getManifest(
      const string& path,
      const Option<string>& tag,
      const Duration& timeout);

  Future<size_t> getBlob(
      const string& path,
      const Option<string>& digest,
      const Path& filePath,
      const Duration& timeout,
      size_t maxSize);

private:
  RegistryClientProcess(
    const Owned<TokenManager>& tokenMgr,
    const URL& registryServer,
    const Option<RegistryClient::Credentials>& creds);

  Future<Response> doHttpGet(
      const URL& url,
      const Option<hashmap<string, string>>& headers,
      const Duration& timeout,
      bool resend,
      const Option<string>& lastResponse) const;

  Try<hashmap<string, string>> getAuthenticationAttributes(
      const Response& httpResponse) const;

  Owned<TokenManager> tokenManager_;
  const URL registryServer_;
  const Option<RegistryClient::Credentials> credentials_;

  RegistryClientProcess(const RegistryClientProcess&) = delete;
  RegistryClientProcess& operator = (const RegistryClientProcess&) = delete;
};


Try<Owned<RegistryClient>> RegistryClient::create(
    const URL& authServer,
    const URL& registryServer,
    const Option<Credentials>& creds)
{
  Try<Owned<RegistryClientProcess>> process =
    RegistryClientProcess::create(authServer, registryServer, creds);

  if (process.isError()) {
    return Error(process.error());
  }

  return Owned<RegistryClient>(
      new RegistryClient(authServer, registryServer, creds, process.get()));
}


RegistryClient::RegistryClient(
    const URL& authServer,
    const URL& registryServer,
    const Option<Credentials>& creds,
    const Owned<RegistryClientProcess>& process)
  : authServer_(authServer),
    registryServer_(registryServer),
    credentials_(creds),
    process_(process)
{
  spawn(CHECK_NOTNULL(process_.get()));
}


RegistryClient::~RegistryClient()
{
  terminate(process_.get());
  process::wait(process_.get());
}


Future<ManifestResponse> RegistryClient::getManifest(
    const string& _path,
    const Option<string>& _tag,
    const Option<Duration>& _timeout)
{
  Duration timeout = _timeout.getOrElse(DEFAULT_MANIFEST_TIMEOUT_SECS);

  return dispatch(
      process_.get(),
      &RegistryClientProcess::getManifest,
      _path,
      _tag,
      timeout);
}


Future<size_t> RegistryClient::getBlob(
    const string& _path,
    const Option<string>& _digest,
    const Path& _filePath,
    const Option<Duration>& _timeout,
    const Option<size_t>& _maxSize)
{
  Duration timeout = _timeout.getOrElse(DEFAULT_MANIFEST_TIMEOUT_SECS);
  size_t maxSize = _maxSize.getOrElse(DEFAULT_MANIFEST_MAXSIZE_BYTES);

  return dispatch(
        process_.get(),
        &RegistryClientProcess::getBlob,
        _path,
        _digest,
        _filePath,
        timeout,
        maxSize);
}


Try<Owned<RegistryClientProcess>> RegistryClientProcess::create(
    const URL& authServer,
    const URL& registryServer,
    const Option<RegistryClient::Credentials>& creds)
{
  Try<Owned<TokenManager>> tokenMgr = TokenManager::create(authServer);
  if (tokenMgr.isError()) {
    return Error("Failed to create token manager: " + tokenMgr.error());
  }

  return Owned<RegistryClientProcess>(
      new RegistryClientProcess(tokenMgr.get(), registryServer, creds));
}


RegistryClientProcess::RegistryClientProcess(
    const Owned<TokenManager>& tokenMgr,
    const URL& registryServer,
    const Option<RegistryClient::Credentials>& creds)
  : tokenManager_(tokenMgr),
    registryServer_(registryServer),
    credentials_(creds) {}


Try<hashmap<string, string>>
RegistryClientProcess::getAuthenticationAttributes(
    const Response& httpResponse) const
{
  if (httpResponse.headers.find("WWW-Authenticate") ==
      httpResponse.headers.end()) {
    return Error("Failed to find WWW-Authenticate header value");
  }

  const string& authString = httpResponse.headers.at("WWW-Authenticate");

  const vector<string> authStringTokens = strings::tokenize(authString, " ");
  if ((authStringTokens.size() != 2) || (authStringTokens[0] != "Bearer")) {
    // TODO(jojy): Look at various possibilities of auth response. We currently
    // assume that the string will have realm information.
    return Error("Invalid authentication header value: " + authString);
  }

  const vector<string> authParams = strings::tokenize(authStringTokens[1], ",");

  hashmap<string, string> authAttributes;
  auto addAttribute = [&authAttributes](
      const string& param) -> Try<Nothing> {
    const vector<string> paramTokens =
      strings::tokenize(param, "=\"");

    if (paramTokens.size() != 2) {
      return Error(
          "Failed to get authentication attribute from response parameter " +
          param);
    }

    authAttributes.insert({paramTokens[0], paramTokens[1]});

    return Nothing();
  };

  foreach (const string& param, authParams) {
    Try<Nothing> addRes = addAttribute(param);
    if (addRes.isError()) {
      return Error(addRes.error());
    }
  }

  return authAttributes;
}


Future<Response>
RegistryClientProcess::doHttpGet(
    const URL& url,
    const Option<hashmap<string, string>>& headers,
    const Duration& timeout,
    bool resend,
    const Option<string>& lastResponseStatus) const
{
  return process::http::get(url, headers)
    .after(timeout, [](
        const Future<Response>& httpResponseFuture) -> Future<Response> {
      return Failure("Response timeout");
    })
    .then(defer(self(), [=](
        const Response& httpResponse) -> Future<Response> {
      VLOG(1) << "Response status: " + httpResponse.status;

      // Set the future if we get a OK response.
      if (httpResponse.status == "200 OK") {
        return httpResponse;
      }

      // Prevent infinite recursion.
      if (lastResponseStatus.isSome() &&
          (lastResponseStatus.get() == httpResponse.status)) {
        return Failure("Invalid response: " + httpResponse.status);
      }

      // If resend is not set, we dont try again and stop here.
      if (!resend) {
        return Failure("Bad response: " + httpResponse.status);
      }

      // Handle 401 Unauthorized.
      if (httpResponse.status == "401 Unauthorized") {
        Try<hashmap<string, string>> authAttributes =
          getAuthenticationAttributes(httpResponse);

        if (authAttributes.isError()) {
          return Failure(
              "Failed to get authentication attributes: " +
              authAttributes.error());
        }

        // TODO(jojy): Currently only handling TLS/cert authentication.
        Future<Token> tokenResponse = tokenManager_->getToken(
          authAttributes.get().at("service"),
          authAttributes.get().at("scope"),
          None());

        return tokenResponse
          .after(timeout, [=](
              Future<Token> tokenResponse) -> Future<Token> {
            tokenResponse.discard();
            return Failure("Token response timeout");
          })
          .then(defer(self(), [=](
              const Future<Token>& tokenResponse) {
            // Send request with acquired token.
            hashmap<string, string> authHeaders = {
              {"Authorization", "Bearer " + tokenResponse.get().raw}
            };

            return doHttpGet(
                url,
                authHeaders,
                timeout,
                true,
                httpResponse.status);
        }));
      } else if (httpResponse.status == "307 Temporary Redirect") {
        // Handle redirect.

        // TODO(jojy): Add redirect functionality in http::get.

        auto toURL = [](
            const string& urlString) -> Try<URL> {
          // TODO(jojy): Need to add functionality to URL class that parses a
          // string to its URL components. For now, assuming:
          //  - scheme is https
          //  - path always ends with /

          static const string schemePrefix = "https://";

          if (!strings::contains(urlString, schemePrefix)) {
            return Error(
                "Failed to find expected token '" + schemePrefix +
                "' in redirect url");
          }

          const string schemeSuffix = urlString.substr(schemePrefix.length());

          const vector<string> components =
            strings::tokenize(schemeSuffix, "/");

          const string path = schemeSuffix.substr(components[0].length());

          const vector<string> addrComponents =
            strings::tokenize(components[0], ":");

          uint16_t port = DEFAULT_SSL_PORT;
          string domain = components[0];

          // Parse the port.
          if (addrComponents.size() == 2) {
            domain = addrComponents[0];

            Try<uint16_t> tryPort = numify<uint16_t>(addrComponents[1]);
            if (tryPort.isError()) {
              return Error(
                  "Failed to parse location: " + urlString + " for port.");
            }

            port = tryPort.get();
          }

          return URL("https", domain, port, path);
        };

        if (httpResponse.headers.find("Location") ==
            httpResponse.headers.end()) {
          return Failure(
              "Invalid redirect response: 'Location' not found in headers.");
        }

        const string& location = httpResponse.headers.at("Location");
        Try<URL> tryUrl = toURL(location);
        if (tryUrl.isError()) {
          return Failure(
              "Failed to parse '" + location + "': " + tryUrl.error());
        }

        return doHttpGet(
            tryUrl.get(),
            headers,
            timeout,
            false,
            httpResponse.status);
      } else {
        return Failure("Invalid response: " + httpResponse.status);
      }
    }));
}


Future<ManifestResponse> RegistryClientProcess::getManifest(
    const string& path,
    const Option<string>& tag,
    const Duration& timeout)
{
  //TODO(jojy): These validations belong in the URL class.
  if (strings::contains(path, " ")) {
    return Failure("Invalid repository path: " + path);
  }

  string repoTag = tag.getOrElse("latest");
  if (strings::contains(repoTag, " ")) {
    return Failure("Invalid repository tag: " + repoTag);
  }

  URL manifestURL(registryServer_);
  manifestURL.path =
    "v2/" + path + "/manifests/" + repoTag;

  auto getManifestResponse = [](
      const Response& httpResponse) -> Try<ManifestResponse> {
    Try<JSON::Object> responseJSON =
      JSON::parse<JSON::Object>(httpResponse.body);

    if (responseJSON.isError()) {
      return Error(responseJSON.error());
    }

    if (!httpResponse.headers.contains("Docker-Content-Digest")) {
      return Error("Docker-Content-Digest header missing in response");
    }

    Result<JSON::String> name = responseJSON.get().find<JSON::String>("name");
    if (name.isNone()) {
      return Error("Failed to find \"name\" in manifest response");
    }

    Result<JSON::Array> fsLayers =
      responseJSON.get().find<JSON::Array>("fsLayers");

    if (fsLayers.isNone()) {
      return Error("Failed to find \"fsLayers\" in manifest response");
    }

    vector<FileSystemLayerInfo> fsLayerInfoList;
    foreach(const JSON::Value& layer, fsLayers.get().values) {
      const JSON::Object& layerInfoJSON = layer.as<JSON::Object>();
      Result<JSON::String> blobSumInfo =
        layerInfoJSON.find<JSON::String>("blobSum");

      if (blobSumInfo.isNone()) {
        return Error("Failed to find \"blobSum\" in manifest response");
      }

      fsLayerInfoList.emplace_back(
          FileSystemLayerInfo{blobSumInfo.get().value});
    }

    return ManifestResponse {
      name.get().value,
      httpResponse.headers.at("Docker-Content-Digest"),
      fsLayerInfoList,
    };
  };

  return doHttpGet(manifestURL, None(), timeout, true, None())
    .then([getManifestResponse] (
        const Future<Response>&  httpResponseFuture
        ) -> Future<ManifestResponse> {
      Try<ManifestResponse> manifestResponse =
        getManifestResponse(httpResponseFuture.get());

      if (manifestResponse.isError()) {
        return Failure(
            "Failed to parse manifest response: " + manifestResponse.error());
      }

      return manifestResponse.get();
    });
}


Future<size_t> RegistryClientProcess::getBlob(
    const string& path,
    const Option<string>& digest,
    const Path& filePath,
    const Duration& timeout,
    size_t maxSize)
{
  auto prepare = ([&filePath]() -> Try<Nothing> {
      const string dirName = filePath.dirname();

      //TODO(jojy): Return more state, for example - if the directory is new.
      Try<Nothing> dirResult = os::mkdir(dirName, true);
      if (dirResult.isError()) {
        return Error(
            "Failed to create directory to download blob: " +
            dirResult.error());
      }

      return dirResult;
  })();

  // TODO(jojy): This currently leaves a residue in failure cases. Would be
  // ideal if we can completely rollback.
  if (prepare.isError()) {
     return Failure(prepare.error());
  }

  if (strings::contains(path, " ")) {
    return Failure("Invalid repository path: " + path);
  }

  URL blobURL(registryServer_);
  blobURL.path =
    "v2/" + path + "/blobs/" + digest.getOrElse("");

  auto saveBlob = [filePath](
      const Response& httpResponse) -> Try<size_t> {
    Try<Nothing> writeResult =
      os::write(filePath, httpResponse.body);

    // TODO(jojy): Add verification step.
    // TODO(jojy): Add check for max size.

    if (writeResult.isError()) {
      return Error(writeResult.error());
    }

    return httpResponse.body.length();
  };

  return doHttpGet(blobURL, None(), timeout, true, None())
    .then([saveBlob](
        const Future<Response>&  httpResponseFuture) -> Future<size_t> {
      Try<size_t> blobSaved = saveBlob(httpResponseFuture.get());
      if (blobSaved.isError()) {
        return Failure("Failed to save blob: " + blobSaved.error());
      }

     return blobSaved.get();
    });
}

} // namespace registry {
} // namespace docker {
} // namespace slave {
} // namespace internal {
} // namespace mesos {
