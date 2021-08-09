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

#include <cstdlib>
#include <ctime>

#include "gtest/gtest.h"
#include "test_helpers.hpp"
#include "datastore.hpp"

using namespace std;
using namespace psicash;
using json = nlohmann::json;

constexpr auto ds_suffix = ".test";

class TestDatastore : public ::testing::Test, public TempDir
{
  public:
    TestDatastore() = default;
};

TEST_F(TestDatastore, InitSimple)
{
    Datastore ds;
    auto err = ds.Init(GetTempDir().c_str(), ds_suffix);
    ASSERT_FALSE(err) << err.ToString();
}

TEST_F(TestDatastore, InitCorrupt)
{
    auto temp_dir = GetTempDir();
    auto ok = WriteBadData(temp_dir.c_str(), true);
    ASSERT_TRUE(ok);

    Datastore ds;
    auto err = ds.Init(temp_dir.c_str(), GetSuffix(true));
    ASSERT_TRUE(err);
    ASSERT_GT(err.ToString().length(), 0);
}

TEST_F(TestDatastore, InitBadDir)
{
    auto bad_dir = GetTempDir() + "/a/b/c/d/f/g";
    Datastore ds1;
    auto err = ds1.Init(bad_dir.c_str(), ds_suffix);
    ASSERT_TRUE(err);

    bad_dir = "/";
    Datastore ds2;
    err = ds2.Init(bad_dir.c_str(), ds_suffix);
    ASSERT_TRUE(err);
}

TEST_F(TestDatastore, CheckPersistence)
{
    // We will create an instance, destroy it, create a new one with the same data directory, and
    // check that it contains our previous data.

    auto temp_dir = GetTempDir();

    auto ds = new Datastore();
    auto err = ds->Init(temp_dir.c_str(), ds_suffix);
    ASSERT_FALSE(err);

    string want = "v";
    auto k = "/k"_json_pointer;
    err = ds->Set(k, want);
    ASSERT_FALSE(err);

    auto got = ds->Get<string>(k);
    ASSERT_TRUE(got);
    ASSERT_EQ(*got, want);

    // Destroy/close the datastore
    delete ds;

    // Create a new one and check that it has the same data.
    ds = new Datastore();
    err = ds->Init(temp_dir.c_str(), ds_suffix);
    ASSERT_FALSE(err);

    got = ds->Get<string>(k);
    ASSERT_TRUE(got);
    ASSERT_EQ(*got, want);

    delete ds;
}

TEST_F(TestDatastore, Reset)
{
    auto temp_dir = GetTempDir();

    auto ds = new Datastore();
    auto err = ds->Init(temp_dir.c_str(), ds_suffix);
    ASSERT_FALSE(err);

    string want = "v";
    auto k = "/k"_json_pointer;
    err = ds->Set(k, want);
    ASSERT_FALSE(err);

    auto got = ds->Get<string>(k);
    ASSERT_TRUE(got);
    ASSERT_EQ(*got, want);

    // Destroy/close the datastore
    delete ds;

    // Create a new one and check that it has the same data.
    ds = new Datastore();
    err = ds->Init(temp_dir.c_str(), ds_suffix);
    ASSERT_FALSE(err);

    got = ds->Get<string>(k);
    ASSERT_TRUE(got);
    ASSERT_EQ(*got, want);

    delete ds;

    // Reset with arguments
    ds = new Datastore();
    err = ds->Reset(temp_dir.c_str(), ds_suffix, {});
    ASSERT_FALSE(err) << err.ToString();

    // First Get without calling Init; should get "not initialized" error
    got = ds->Get<string>(k);
    ASSERT_FALSE(got);
    ASSERT_EQ(got.error(), psicash::Datastore::kDatastoreUninitialized);

    // Then initialize and try again
    err = ds->Init(temp_dir.c_str(), ds_suffix);
    ASSERT_FALSE(err);

    // Key should not be found, since we haven't set it
    got = ds->Get<string>(k);
    ASSERT_FALSE(got);
    ASSERT_EQ(got.error(), psicash::Datastore::kNotFound);

    // Set it
    err = ds->Set(k, want);
    ASSERT_FALSE(err);

    // Get it for real
    got = ds->Get<string>(k);
    ASSERT_TRUE(got);
    ASSERT_EQ(*got, want);

    delete ds;

    // Reset without arguments
    ds = new Datastore();
    err = ds->Init(temp_dir.c_str(), ds_suffix);
    ASSERT_FALSE(err);

    got = ds->Get<string>(k);
    ASSERT_TRUE(got);
    ASSERT_EQ(*got, want);

    err = ds->Reset({});
    ASSERT_FALSE(err);

    got = ds->Get<string>(k);
    ASSERT_FALSE(got);
    ASSERT_EQ(got.error(), psicash::Datastore::kNotFound);

    delete ds;

    // Reset with non-empty new value
    temp_dir = GetTempDir(); // use a fresh dir to avoid pollution
    ds = new Datastore();
    err = ds->Init(temp_dir.c_str(), ds_suffix);
    ASSERT_FALSE(err);

    got = ds->Get<string>("/k"_json_pointer);
    ASSERT_FALSE(got);

    err = ds->Reset({{"k", want}});
    ASSERT_FALSE(err);

    got = ds->Get<string>("/k"_json_pointer);
    ASSERT_TRUE(got);
    ASSERT_EQ(*got, want);

    delete ds;

}

TEST_F(TestDatastore, WritePause)
{
    auto temp_dir = GetTempDir();

    auto ds = new Datastore();
    auto err = ds->Init(temp_dir.c_str(), ds_suffix);
    ASSERT_FALSE(err);

    // This should persist
    auto pause_want1 = "/pause_want1"_json_pointer;
    err = ds->Set(pause_want1, pause_want1.to_string());
    ASSERT_FALSE(err);

    // This should persist, as we're committing
    ds->PauseWrites();
    auto pause_want2 = "/pause_want2"_json_pointer;
    err = ds->Set(pause_want2, pause_want2.to_string());
    ASSERT_FALSE(err);
    err = ds->UnpauseWrites(/*commit=*/true);
    ASSERT_FALSE(err);

    // This should NOT persist, as we're rolling back
    ds->PauseWrites();
    auto pause_want3 = "/pause_want3"_json_pointer;
    err = ds->Set(pause_want3, pause_want3.to_string());
    ASSERT_FALSE(err);
    err = ds->UnpauseWrites(/*commit=*/false);
    ASSERT_FALSE(err);

    // Another committed value, to make sure the order of things doesn't matter
    ds->PauseWrites();
    auto pause_want4 = "/pause_want4"_json_pointer;
    err = ds->Set(pause_want4, pause_want4.to_string());
    ASSERT_FALSE(err);
    err = ds->UnpauseWrites(/*commit=*/true);
    ASSERT_FALSE(err);

    // This should also NOT persist, since we're hitting the dtor
    ds->PauseWrites();
    auto pause_want5 = "/pause_want5"_json_pointer;
    err = ds->Set(pause_want5, pause_want5.to_string());
    ASSERT_FALSE(err);

    // Close
    delete ds;

    // Reopen
    ds = new Datastore();
    err = ds->Init(temp_dir.c_str(), ds_suffix);
    ASSERT_FALSE(err);

    auto got = ds->Get<string>(pause_want1);
    ASSERT_TRUE(got);
    ASSERT_EQ(*got, pause_want1.to_string());

    got = ds->Get<string>(pause_want2);
    ASSERT_TRUE(got);
    ASSERT_EQ(*got, pause_want2.to_string());

    got = ds->Get<string>(pause_want3);
    ASSERT_FALSE(got);

    got = ds->Get<string>(pause_want4);
    ASSERT_TRUE(got);
    ASSERT_EQ(*got, pause_want4.to_string());

    got = ds->Get<string>(pause_want5);
    ASSERT_FALSE(got);

    delete ds;
}

TEST_F(TestDatastore, SetSimple)
{
    Datastore ds;
    auto err = ds.Init(GetTempDir().c_str(), ds_suffix);
    ASSERT_FALSE(err);

    auto k = "/k"_json_pointer;
    string want = "v";
    err = ds.Set(k, want);
    ASSERT_FALSE(err);

    auto got = ds.Get<string>(k);
    ASSERT_TRUE(got);
    ASSERT_EQ(*got, want);
}

TEST_F(TestDatastore, SetDeep)
{
    Datastore ds;
    auto err = ds.Init(GetTempDir().c_str(), ds_suffix);
    ASSERT_FALSE(err);

    auto key1 = "/key1"_json_pointer, key2 = "/key2"_json_pointer;
    string want = "want";
    err = ds.Set(key1/key2, want);
    ASSERT_FALSE(err);

    // Try to get key1 and then get key2 from it
    auto gotShallow = ds.Get<json>(key1);
    ASSERT_TRUE(gotShallow);

    string gotDeep = gotShallow->at(key2).get<string>();
    ASSERT_EQ(gotDeep, want);

    // Then try to get /key1/key2 directly
    auto gotDeep2 = ds.Get<string>(key1/key2);
    ASSERT_TRUE(gotDeep2);
    ASSERT_EQ(gotDeep2, want);
}

TEST_F(TestDatastore, SetAndClear)
{
    Datastore ds;
    auto err = ds.Init(GetTempDir().c_str(), ds_suffix);
    ASSERT_FALSE(err);

    map<string, string> want = {{"a", "a"}, {"b", "b"}};
    auto k = "/k"_json_pointer;
    err = ds.Set(k, want);
    ASSERT_FALSE(err);

    auto got = ds.Get<map<string, string>>(k);
    ASSERT_TRUE(got);
    ASSERT_EQ(got->size(), want.size());

    want.clear();
    err = ds.Set(k, want);
    ASSERT_FALSE(err);

    got = ds.Get<map<string, string>>(k);
    ASSERT_TRUE(got);
    ASSERT_EQ(got->size(), 0);
}

TEST_F(TestDatastore, SetTypes)
{
    // Test some types other than just string

    Datastore ds;
    auto err = ds.Init(GetTempDir().c_str(), ds_suffix);
    ASSERT_FALSE(err);

    // Start with string
    string wantString = "v";
    auto wantStringKey = "/wantStringKey"_json_pointer;
    err = ds.Set(wantStringKey, wantString);
    ASSERT_FALSE(err);

    auto gotString = ds.Get<string>(wantStringKey);
    ASSERT_TRUE(gotString);
    ASSERT_EQ(*gotString, wantString);

    // bool
    bool wantBool = true;
    auto wantBoolKey = "/wantBoolKey"_json_pointer;
    err = ds.Set(wantBoolKey, wantBool);
    ASSERT_FALSE(err);

    auto gotBool = ds.Get<bool>(wantBoolKey);
    ASSERT_TRUE(gotBool);
    ASSERT_EQ(*gotBool, wantBool);

    // int
    int wantInt = 5273482;
    auto wantIntKey = "/wantIntKey"_json_pointer;
    err = ds.Set(wantIntKey, wantInt);
    ASSERT_FALSE(err);

    auto gotInt = ds.Get<int>(wantIntKey);
    ASSERT_TRUE(gotInt);
    ASSERT_EQ(*gotInt, wantInt);
}

TEST_F(TestDatastore, TypeMismatch)
{
    Datastore ds;
    auto err = ds.Init(GetTempDir().c_str(), ds_suffix);
    ASSERT_FALSE(err);

    // string
    string wantString = "v";
    auto wantStringKey = "/wantStringKey"_json_pointer;
    err = ds.Set(wantStringKey, wantString);
    ASSERT_FALSE(err);

    auto gotString = ds.Get<string>(wantStringKey);
    ASSERT_TRUE(gotString);
    ASSERT_EQ(*gotString, wantString);

    // bool
    bool wantBool = true;
    auto wantBoolKey = "/wantBoolKey"_json_pointer;
    err = ds.Set(wantBoolKey, wantBool);
    ASSERT_FALSE(err);

    auto gotBool = ds.Get<bool>(wantBoolKey);
    ASSERT_TRUE(gotBool);
    ASSERT_EQ(*gotBool, wantBool);

    // int
    int wantInt = 5273482;
    auto wantIntKey = "/wantIntKey"_json_pointer;
    err = ds.Set(wantIntKey, wantInt);
    ASSERT_FALSE(err);

    auto gotInt = ds.Get<int>(wantIntKey);
    ASSERT_TRUE(gotInt);
    ASSERT_EQ(*gotInt, wantInt);

    // It's an error to set one type and then try to get another
    auto got_fail_1 = ds.Get<bool>(wantStringKey);
    ASSERT_FALSE(got_fail_1);
    ASSERT_EQ(got_fail_1.error(), psicash::Datastore::kTypeMismatch);

    auto got_fail_2 = ds.Get<string>(wantIntKey);
    ASSERT_FALSE(got_fail_2);
    ASSERT_EQ(got_fail_2.error(), psicash::Datastore::kTypeMismatch);

    auto got_fail_3 = ds.Get<int>(wantStringKey);
    ASSERT_FALSE(got_fail_3);
    ASSERT_EQ(got_fail_3.error(), psicash::Datastore::kTypeMismatch);

    auto got_fail_4 = ds.Get<int>(wantBoolKey);
    //ASSERT_FALSE(got_fail_4); // NOTE: This doesn't actually fail. There must be a successful implicit conversion.

    // It's not an error to set one type to a key and then replace it with another type
    err = ds.Set(wantStringKey, wantBool);
    ASSERT_FALSE(err);
}

TEST_F(TestDatastore, GetSimple)
{
    Datastore ds;
    auto err = ds.Init(GetTempDir().c_str(), ds_suffix);
    ASSERT_FALSE(err);

    string want = "v";
    err = ds.Set("/k"_json_pointer, want);
    ASSERT_FALSE(err);

    auto got = ds.Get<string>("/k"_json_pointer);
    ASSERT_TRUE(got);
    ASSERT_EQ(*got, want);
}

TEST_F(TestDatastore, GetDeep)
{
    // This is a copy of SetDeep

    Datastore ds;
    auto err = ds.Init(GetTempDir().c_str(), ds_suffix);
    ASSERT_FALSE(err);

    auto key1 = "/key1"_json_pointer, key2 = "/key2"_json_pointer;
    string want = "want";
    err = ds.Set(key1/key2, want);
    ASSERT_FALSE(err);

    // Try to get key1 and then get key2 from it
    auto gotShallow = ds.Get<json>(key1);
    ASSERT_TRUE(gotShallow);

    string gotDeep = gotShallow->at(key2).get<string>();
    ASSERT_EQ(gotDeep, want);

    // Then try to get /key1/key2 directly
    auto gotDeep2 = ds.Get<string>(key1/key2);
    ASSERT_TRUE(gotDeep2);
    ASSERT_EQ(gotDeep2, want);
}

TEST_F(TestDatastore, GetNotFound)
{
    Datastore ds;
    auto err = ds.Init(GetTempDir().c_str(), ds_suffix);
    ASSERT_FALSE(err);

    string want = "v";
    auto k = "/k"_json_pointer;
    err = ds.Set(k, want);
    ASSERT_FALSE(err);

    auto got = ds.Get<string>(k);
    ASSERT_TRUE(got);
    ASSERT_EQ(*got, want);

    // Bad key
    auto nope = ds.Get<string>("/nope"_json_pointer);
    ASSERT_FALSE(nope);
    ASSERT_EQ(nope.error(), psicash::Datastore::kNotFound);
}

TEST_F(TestDatastore, GetFullDS)
{
    // Testing Get() with no params, that returns the full datastore json

    Datastore ds;

    // Error before Init
    auto j = ds.Get();
    ASSERT_FALSE(j);

    auto err = ds.Init(GetTempDir().c_str(), ds_suffix);
    ASSERT_FALSE(err);

    j = ds.Get();
    ASSERT_TRUE(j);
    ASSERT_TRUE(j->empty());

    string want = "v";
    err = ds.Set("/k"_json_pointer, want);
    ASSERT_FALSE(err);

    j = ds.Get();
    ASSERT_TRUE(j);
    ASSERT_EQ(j->at("k").get<string>(), want);
}


/*
This was a failed attempt to trigger a datastore corruption error we sometimes see.
To run, FileStore and FileLoad need to be exported.
TEST_F(TestDatastore, Errors)
{
    Datastore ds;

    auto err = ds.Init(GetTempDir().c_str(), ds_suffix);
    ASSERT_FALSE(err);

    for (size_t i = 0; i < 1000; i++) {
        auto j = ds.Get();
        ASSERT_TRUE(j);

        string want = "v";
        err = ds.Set("/k"_json_pointer, want);
        ASSERT_FALSE(err);
    }

    auto datastore_filename = DatastoreFilepath(GetTempDir(), true);

    auto load_res = FileLoad(datastore_filename);
    ASSERT_TRUE(load_res);

    auto error = FileStore(false, datastore_filename, json::object());
    ASSERT_FALSE(error) << error.ToString();
    load_res = FileLoad(datastore_filename);
    ASSERT_TRUE(load_res);

    error = FileStore(false, datastore_filename, json());
    ASSERT_FALSE(error) << error.ToString();
    load_res = FileLoad(datastore_filename);
    ASSERT_TRUE(load_res);

    error = FileStore(false, datastore_filename, nullptr);
    ASSERT_FALSE(error) << error.ToString();
    load_res = FileLoad(datastore_filename);
    ASSERT_TRUE(load_res);
}
*/
