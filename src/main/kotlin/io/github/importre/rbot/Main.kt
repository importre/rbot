package io.github.importre.rbot

import org.apache.commons.lang.StringUtils
import org.apache.mahout.cf.taste.impl.model.jdbc.PostgreSQLJDBCDataModel
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDPlusPlusFactorizer
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity
import org.apache.mahout.cf.taste.recommender.RecommendedItem
import org.postgresql.ds.PGSimpleDataSource
import java.util.ArrayList

fun main(args: Array<String>) {
    println(BuildConfig.VERSION)

    val dataSource = PGSimpleDataSource()
    dataSource.setServerName("localhost")
    dataSource.setDatabaseName(BuildConfig.DB_NAME)

    val conn = dataSource.getConnection()
    val stmt = conn.createStatement()
    stmt.execute("DROP TABLE IF EXISTS temp")
    stmt.execute("""
        CREATE TABLE temp (
            user_id BIGINT NOT NULL,
            item_id BIGINT NOT NULL,
            preference REAL NOT NULL,
            PRIMARY KEY (user_id, item_id)
        )
    """)
    stmt.execute(BuildConfig.INSERT_SQL)

    val model = PostgreSQLJDBCDataModel(dataSource,
            "temp", "user_id", "item_id", "preference", null)

    val recommendedItems = getRecommends(model, 2)
    val ids = ArrayList<String>()
    for (item in recommendedItems) {
        ids.add("t.id=" + item.getItemID())
    }

    if (!ids.isEmpty()) {
        val s = StringUtils.join(ids, " OR ")
        val sql = java.lang.String.format(BuildConfig.SELECT_SQL, s)

        val res = stmt.executeQuery(sql)
        while (res.next()) {
            val t = res.getString(1)
            val p = res.getString(2)
            val n = res.getString(3)
            println(t + " p." + p + " #" + n)
        }
    }

    stmt.close()
    conn.close()

    println("done")
}

fun getRecommends(model: PostgreSQLJDBCDataModel, method: Int): List<RecommendedItem> {
    val id: Long = 1327
    val howMany = 5
    val iter = 5

    when (method) {
        0 -> {
            val similarity = EuclideanDistanceSimilarity(model)
            val neighborhood = NearestNUserNeighborhood(10, similarity, model)
            val recommender = GenericUserBasedRecommender(model, neighborhood, similarity)
            return recommender.recommend(id, howMany)
        }

        1 -> {
            val similarity = EuclideanDistanceSimilarity(model)
            val recommender = GenericItemBasedRecommender(model, similarity)
            return recommender.recommend(id, howMany)
        }

        2 -> {
            val factorizer = SVDPlusPlusFactorizer(model, 3, iter)
            val svdRecommender = SVDRecommender(model, factorizer)
            return svdRecommender.recommend(id, howMany)
        }

        3 -> {
            val factorizer = ALSWRFactorizer(model, 3, 0.5, iter)
            val svdRecommender = SVDRecommender(model, factorizer)
            return svdRecommender.recommend(id, howMany)
        }

        else -> return arrayListOf()
    }
}
