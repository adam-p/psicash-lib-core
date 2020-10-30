/*
 * Copyright (c) 2018, Psiphon Inc.
 * All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#include "userdata.hpp"
#include "datastore.hpp"
#include "psicash.hpp"
#include "vendor/nlohmann/json.hpp"

using json = nlohmann::json;

using namespace std;

namespace psicash {

constexpr int kCurrentDatastoreVersion = 2;

// Datastore keys
static constexpr const char* VERSION = "v";
static auto kVersionPtr = "/v"_json_pointer;
//
// Instance-specific data keys
//
static auto kInstancePtr = "/instance"_json_pointer;
static const string INSTANCE_ID = "instanceID";
static const auto kInstanceIDPtr = kInstancePtr / INSTANCE_ID;
static constexpr const char* IS_LOGGED_OUT_ACCOUNT = "isLoggedOutAccount";
static const auto kIsLoggedOutAccountPtr = kInstancePtr / IS_LOGGED_OUT_ACCOUNT;
//
// User-specific data keys
//
static auto kUserPtr = "/user"_json_pointer;
static constexpr const char* SERVER_TIME_DIFF = "serverTimeDiff";
static const auto kServerTimeDiffPtr = kUserPtr / SERVER_TIME_DIFF;
static constexpr const char* AUTH_TOKENS = "authTokens";
static const auto kAuthTokensPtr = kUserPtr / AUTH_TOKENS;
static constexpr const char* AUTH_TOKENS_TIMESTAMP = "authTokensTimestamp";
static const auto kAuthTokensTimestampPtr = kUserPtr / AUTH_TOKENS_TIMESTAMP;
static constexpr const char* BALANCE = "balance";
static const auto kBalancePtr = kUserPtr / BALANCE;
static constexpr const char* IS_ACCOUNT = "isAccount";
static const auto kIsAccountPtr = kUserPtr / IS_ACCOUNT;
static constexpr const char* PURCHASE_PRICES = "purchasePrices";
static const auto kPurchasePricesPtr = kUserPtr / PURCHASE_PRICES;
static constexpr const char* PURCHASES = "purchases";
static const auto kPurchasesPtr = kUserPtr / PURCHASES;
static constexpr const char* LAST_TRANSACTION_ID = "lastTransactionID";
static const auto kLastTransactionIDPtr = kUserPtr / LAST_TRANSACTION_ID;
static const char* REQUEST_METADATA = "requestMetadata";
const json::json_pointer kRequestMetadataPtr = kUserPtr / REQUEST_METADATA; // used in header, so not static


// These are the possible token types.
const char* const kEarnerTokenType = "earner";
const char* const kSpenderTokenType = "spender";
const char* const kIndicatorTokenType = "indicator";
const char* const kAccountTokenType = "account";
const char* const kLogoutTokenType = "logout";


UserData::UserData() {
}

UserData::~UserData() {
}

static string DataStoreSuffix(bool dev) {
    return dev ? ".dev" : ".prod";
}

static auto FreshDatastore() {
    json ds;
    ds[kVersionPtr] = kCurrentDatastoreVersion;
    ds[kUserPtr] = json::object();
    ds[kInstancePtr] = json::object();
    ds[kInstanceIDPtr] = "instanceid_"s + utils::RandomID();

    return ds;
}

error::Error UserData::Init(const string& file_store_root, bool dev) {
    auto err = datastore_.Init(file_store_root, DataStoreSuffix(dev));
    if (err) {
        return PassError(err);
    }

    auto version = datastore_.Get<int>(kVersionPtr);
    if (!version) {
        err = datastore_.Reset(FreshDatastore());
        if (err) {
            return PassError(err);
        }
    }
    else if (*version == 1) {
        // We need to migrate from the structure where all data was at the root
        // of the object, to one where the object looks like:
        // {"v":2,"user":{old data},"instance":{new stuff}}

        auto oldDS = datastore_.Get();
        if (!oldDS) {
            // This should never happen. The version was successfully returned,
            // so we know there's a structure there and we should have got it.
            return error::MakeCriticalError("failed to retrieve v1 data");
        }

        json newDS = FreshDatastore();
        (*oldDS).erase("v");
        newDS[kUserPtr] = *oldDS;

        // The v1 datastore also didn't have AuthTokenTimestamp.
        // So set that to "now". We'll do so without checking if there are tokens, since
        // there's no downside.
        newDS[kAuthTokensTimestampPtr] = datetime::DateTime::Now().ToISO8601();

        err = datastore_.Reset(newDS);
        if (err) {
            return PassError(err);
        }
    }
    else if (*version != kCurrentDatastoreVersion) {
        return error::MakeCriticalError(
                utils::Stringer("found unexpected version number: ", *version));
    }
    // else we've loaded a good, current datastore

    return error::nullerr;
}

error::Error UserData::Clear(const string& file_store_root, bool dev) {
    return PassError(datastore_.Reset(
        file_store_root, DataStoreSuffix(dev), FreshDatastore()));
}

error::Error UserData::Clear() {
    return PassError(datastore_.Reset(FreshDatastore()));
}

error::Error UserData::DeleteUserData(bool isLoggedOutAccout) {
    WritePauser pauser(*this);
    // Not checking return values, since writing is paused.
    (void)datastore_.Set(kUserPtr, json::object());
    (void)SetIsLoggedOutAccount(isLoggedOutAccout);
    return PassError(pauser.Commit());
}

std::string UserData::GetInstanceID() const {
    auto v = datastore_.Get<string>(kInstanceIDPtr);

    // This should not happen. The instance ID must be initialized when the datastore is set up.
    assert(!!v);

    return *v;
}

bool UserData::GetIsLoggedOutAccount() const {
    auto v = datastore_.Get<bool>(kIsLoggedOutAccountPtr);
    if (!v) {
        return false;
    }
    return *v;
}

error::Error UserData::SetIsLoggedOutAccount(bool v) {
    return PassError(datastore_.Set(kIsLoggedOutAccountPtr, v));
}

datetime::Duration UserData::GetServerTimeDiff() const {
    auto v = datastore_.Get<int64_t>(kServerTimeDiffPtr);
    if (!v) {
        return datetime::DurationFromInt64(0);
    }
    return datetime::DurationFromInt64(*v);
}

error::Error UserData::SetServerTimeDiff(const datetime::DateTime& serverTimeNow) {
    auto localTimeNow = datetime::DateTime::Now();
    auto diff = serverTimeNow.Diff(localTimeNow);
    return PassError(datastore_.Set(kServerTimeDiffPtr, datetime::DurationToInt64(diff)));
}

AuthTokens UserData::GetAuthTokens() const {
    auto v = datastore_.Get<AuthTokens>(kAuthTokensPtr);
    if (!v) {
        return AuthTokens();
    }
    return *v;
}

std::string UserData::GetAuthTokensTimestamp() const {
    auto v = datastore_.Get<string>(kAuthTokensTimestampPtr);
    if (!v) {
        // This should only happen if there are no tokens.
        assert(GetAuthTokens().empty());
        // ...But if it did happen when it shouldn't have, the sanest thing to do is
        // return the current timestamp.
        return datetime::DateTime::Now().ToISO8601();
    }

    return *v;
}

error::Error UserData::SetAuthTokens(const AuthTokens& v, const std::string& timestamp,
                                     bool is_account) {
    WritePauser pauser(*this);
    // Not checking errors while paused, as there's no error that can occur.
    (void)datastore_.Set(kAuthTokensPtr, v);
    (void)datastore_.Set(kAuthTokensTimestampPtr, timestamp);
    (void)datastore_.Set(kIsAccountPtr, is_account);
    return PassError(pauser.Commit()); // write
}

error::Error UserData::CullAuthTokens(const std::map<std::string, bool>& valid_tokens) {
    auto all_auth_tokens = GetAuthTokens();
    AuthTokens good_auth_tokens;

    // all_auth_tokens is { "earner": "ABCD0123" } and valid_tokens is { "ABCD0123": true }
    for (const auto& t : all_auth_tokens) {
        for (const auto& vtt : valid_tokens) {
            if (vtt.first == t.second && vtt.second) {
                good_auth_tokens[t.first] = t.second;
                break;
            }
        }
    }

    return PassError(datastore_.Set(kAuthTokensPtr, good_auth_tokens));
}

psicash::TokenTypes UserData::ValidTokenTypes() const {
    auto auth_tokens = GetAuthTokens();
    vector<string> valid_token_types;
    std::transform(auth_tokens.begin(), auth_tokens.end(), std::back_inserter(valid_token_types), [](const auto& t) -> string { return t.first; });
    return valid_token_types;
}

bool UserData::GetIsAccount() const {
    auto v = datastore_.Get<bool>(kIsAccountPtr);
    if (!v) {
        return false;
    }
    return *v;
}

error::Error UserData::SetIsAccount(bool v) {
    return PassError(datastore_.Set(kIsAccountPtr, v));
}

int64_t UserData::GetBalance() const {
    auto v = datastore_.Get<int64_t>(kBalancePtr);
    if (!v) {
        return 0;
    }
    return *v;
}

error::Error UserData::SetBalance(int64_t v) {
    return PassError(datastore_.Set(kBalancePtr, v));
}

PurchasePrices UserData::GetPurchasePrices() const {
    auto v = datastore_.Get<PurchasePrices>(kPurchasePricesPtr);
    if (!v) {
        return PurchasePrices();
    }
    return *v;
}

error::Error UserData::SetPurchasePrices(const PurchasePrices& v) {
    return PassError(datastore_.Set(kPurchasePricesPtr, v));
}

Purchases UserData::GetPurchases() const {
    auto v = datastore_.Get<Purchases>(kPurchasesPtr);
    if (!v) {
        v = Purchases();
    }

    UpdatePurchasesLocalTimeExpiry(*v);
    return *v;
}

error::Error UserData::SetPurchases(const Purchases& v) {
    return PassError(datastore_.Set(kPurchasesPtr, v));
}

error::Error UserData::AddPurchase(const Purchase& v) {
    // We're not going to assume too much about the incoming purchase: it might be a
    // duplicate, or it might not be as new as the newest purchase we already have.

    // Assumption: The purchases vector is already sorted by created date ascending.
    // Assumption: The ID of our purchase argument should become our LastTransactionID.
    //   This will be true _even if_ there are purchases in our datastore with later
    //   created dates.
    //   - If this purchase is being added due to, say, NewExpiringPurchase, then the
    //     purchase is brand new.
    //   - If the purchase is being added due to RefreshState retreiving some newer than
    //     our LastTransactionID, then the argument is newer.
    //   - If the purchase is being added because our LastTransactionID was corrupt and
    //     RefreshState is giving us everything, then we need to replace it with the
    //     purchases we're storing, as we store them.

    // TODO: If/when we start dealing with large numbers of purchases being added,
    // (e.g., when we have a large number of a non-instance-specific purchases and the
    // user is doing a full retrieve), the work here should be done more efficiently
    // (like in a batch).
    // (Of course, we might also have to rethink our datastore at that point.)

    auto purchases = GetPurchases();

    for (auto iter = purchases.begin(); ; iter++) {
        if (iter == purchases.end()) {
            // We searched to the end and didn't find a duplicate or insertion point.
            // Put the new purchase at the end.
            purchases.insert(iter, v);
            break;
        }
        else if ((*iter).id == v.id) {
            // This is a duplicate. Update our local copy in case we have bad data.
            *iter = v;
            break;
        }
        else if ((*iter).server_time_created > v.server_time_created) {
            // We have found the sorted insertion point.
            purchases.insert(iter, v);
            break;
        }
    }

    // Pause to set Purchases and LastTransactionID in one write
    WritePauser pauser(*this);
    // These don't write, so have no meaningful return
    (void)SetPurchases(purchases);
    (void)SetLastTransactionID(v.id);
    return PassError(pauser.Commit()); // write
}

void UserData::UpdatePurchaseLocalTimeExpiry(Purchase& purchase) const {
    if (!purchase.server_time_expiry) {
        return;
    }

    // server_time_diff is server-minus-local. So it's positive if server is ahead, negative if behind.
    // So we have to subtract the diff from the server time to get the local time.
    // Δ = s - l
    // l = s - Δ
    purchase.local_time_expiry = purchase.server_time_expiry->Sub(GetServerTimeDiff());
}

void UserData::UpdatePurchasesLocalTimeExpiry(Purchases& purchases) const {
    for (auto& p : purchases) {
        UpdatePurchaseLocalTimeExpiry(p);
    }
}

TransactionID UserData::GetLastTransactionID() const {
    auto v = datastore_.Get<TransactionID>(kLastTransactionIDPtr);
    if (!v) {
        return TransactionID();
    }
    return *v;
}

error::Error UserData::SetLastTransactionID(const TransactionID& v) {
    return PassError(datastore_.Set(kLastTransactionIDPtr, v));
}

json UserData::GetRequestMetadata() const {
    auto j = datastore_.Get<json>(kRequestMetadataPtr);
    if (!j) {
        return json::object();
    }

    return *j;
}

} // namespace psicash
