import uuid
from typing import Optional
from datetime import datetime
from pydantic import BaseModel

from lib.pg import PgConnect
import uuid


class UserCategoryCountersSetting(BaseModel):
    id: int
    user_id: uuid.UUID
    category_id: uuid.UUID
    category_name: str
    order_cnt: int
    
class UserProductCountersSetting(BaseModel):
    id: int
    user_id: uuid.UUID
    product_id: uuid.UUID
    product_name: str
    order_cnt: int

class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_category_insert(self,
                          category_name: str,
                          load_dt: datetime,
                          load_src: str
                          ) -> uuid.UUID:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_category (category_name, load_dt, load_src) 
                        VALUES(%(category_name)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (category_name) DO NOTHING;
                    """,
                    {
                        'category_name': category_name,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )
                cur.execute(
                    """
                        SELECT h_category_pk 
                        FROM dds.h_category 
                        WHERE category_name = %(category_name)s;
                    """,
                    {'category_name': category_name}
                )
                return cur.fetchone()[0]

    def h_order_insert(self,
                       order_id: int,
                       order_dt: datetime,
                       load_dt: datetime,
                       load_src: str
                       ) -> uuid.UUID:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_order (order_id, order_dt, load_dt, load_src) 
                        VALUES(%(order_id)s, %(order_dt)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (order_id) DO UPDATE
                        SET order_dt = EXCLUDED.order_dt;
                    """,
                    {
                        'order_id': order_id,
                        'order_dt': order_dt,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )
                cur.execute(
                    """
                        SELECT h_order_pk 
                        FROM dds.h_order 
                        WHERE order_id = %(order_id)s;
                    """,
                    {'order_id': order_id}
                )
                return cur.fetchone()[0]

    def h_product_insert(self,
                         product_id: int,
                         load_dt: datetime,
                         load_src: str
                         ) -> uuid.UUID:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_product (product_id, load_dt, load_src) 
                        VALUES(%(product_id)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (product_id) DO NOTHING;
                    """,
                    {
                        'product_id': product_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )
                cur.execute(
                    """
                        SELECT h_product_pk 
                        FROM dds.h_product 
                        WHERE product_id = %(product_id)s;
                    """,
                    {'product_id': product_id}
                )
                return cur.fetchone()[0]

    def h_restaurant_insert(self,
                            restaurant_id: int,
                            load_dt: datetime,
                            load_src: str
                            ) -> uuid.UUID:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_restaurant (restaurant_id, load_dt, load_src) 
                        VALUES(%(restaurant_id)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (restaurant_id) DO NOTHING;
                    """,
                    {
                        'restaurant_id': restaurant_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )
                cur.execute(
                    """
                        SELECT h_restaurant_pk 
                        FROM dds.h_restaurant 
                        WHERE restaurant_id = %(restaurant_id)s;
                    """,
                    {'restaurant_id': restaurant_id}
                )
                return cur.fetchone()[0]

    def h_user_insert(self,
                      user_id: int,
                      load_dt: datetime,
                      load_src: str
                      ) -> uuid.UUID:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_user (user_id, load_dt, load_src) 
                        VALUES(%(user_id)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (user_id) DO NOTHING;
                    """,
                    {
                        'user_id': user_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )
                cur.execute(
                    """
                        SELECT h_user_pk 
                        FROM dds.h_user 
                        WHERE user_id = %(user_id)s;
                    """,
                    {'user_id': user_id}
                )
                return cur.fetchone()[0]

    def l_order_product_insert(self,
                               h_order_pk: uuid.UUID,
                               h_product_pk: uuid.UUID,
                               load_dt: datetime,
                               load_src: str
                               ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_product (h_order_pk, h_product_pk, load_dt, load_src) 
                        VALUES(%(h_order_pk)s, %(h_product_pk)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_order_pk, h_product_pk) DO NOTHING;
                    """,
                    {
                        'h_order_pk': h_order_pk,
                        'h_product_pk': h_product_pk,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def l_order_user_insert(self,
                            h_order_pk: uuid.UUID,
                            h_user_pk: uuid.UUID,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_user (h_order_pk, h_user_pk, load_dt, load_src) 
                        VALUES(%(h_order_pk)s, %(h_user_pk)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_order_pk, h_user_pk) DO NOTHING;
                    """,
                    {
                        'h_order_pk': h_order_pk,
                        'h_user_pk': h_user_pk,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def l_product_category_insert(self,
                                  h_product_pk: uuid.UUID,
                                  h_category_pk: uuid.UUID,
                                  load_dt: datetime,
                                  load_src: str
                                  ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_category (h_product_pk, h_category_pk, load_dt, load_src) 
                        VALUES( %(h_product_pk)s, %(h_category_pk)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_product_pk, h_category_pk) DO NOTHING;
                    """,
                    {
                        'h_product_pk': h_product_pk,
                        'h_category_pk': h_category_pk,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def l_product_restaurant_insert(self,
                                    h_product_pk: uuid.UUID,
                                    h_restaurant_pk: uuid.UUID,
                                    load_dt: datetime,
                                    load_src: str
                                    ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_restaurant (h_product_pk, h_restaurant_pk, load_dt, load_src) 
                        VALUES(%(h_product_pk)s, %(h_restaurant_pk)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_product_pk, h_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'h_product_pk': h_product_pk,
                        'h_restaurant_pk': h_restaurant_pk,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def s_order_cost_insert(self,
                            h_order_pk: uuid.UUID,
                            cost: float,
                            payment: float,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_cost (h_order_pk, cost, payment, load_dt, load_src) 
                        VALUES(%(h_order_pk)s, %(cost)s, %(payment)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_order_pk, cost, payment) DO UPDATE
                        SET
                            cost = EXCLUDED.cost,
                            payment = EXCLUDED.payment,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src;
                    """,
                    {
                        'h_order_pk': h_order_pk,
                        'cost': cost,
                        'payment': payment,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def s_order_status_insert(self,
                              h_order_pk: uuid.UUID,
                              status: str,
                              load_dt: datetime,
                              load_src: str
                              ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_status (h_order_pk, status, load_dt, load_src) 
                        VALUES(%(h_order_pk)s, %(status)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_order_pk, status) DO UPDATE
                        SET
                            status = EXCLUDED.status,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src;
                    """,
                    {
                        'h_order_pk': h_order_pk,
                        'status': status,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def s_product_names_insert(self,
                               h_product_pk: uuid.UUID,
                               name: str,
                               load_dt: datetime,
                               load_src: str
                               ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_product_names (h_product_pk, name, load_dt, load_src) 
                        VALUES(%(h_product_pk)s, %(name)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_product_pk, name) DO UPDATE
                        SET
                            name = EXCLUDED.name,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src;
                    """,
                    {
                        'h_product_pk': h_product_pk,
                        'name': name,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def s_restaurant_names_insert(self,
                                  h_restaurant_pk: uuid.UUID,
                                  name: str,
                                  load_dt: datetime,
                                  load_src: str
                                  ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_restaurant_names (h_restaurant_pk, name, load_dt, load_src) 
                        VALUES(%(h_restaurant_pk)s, %(name)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_restaurant_pk, name) DO UPDATE
                        SET
                            name = EXCLUDED.name,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src;
                    """,
                    {
                        'h_restaurant_pk': h_restaurant_pk,
                        'name': name,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def s_user_names_insert(self,
                            h_user_pk: uuid.UUID,
                            username: str,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_user_names (h_user_pk, username, load_dt, load_src) 
                        VALUES(%(h_user_pk)s, %(username)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_user_pk, username) DO UPDATE
                        SET
                            username = EXCLUDED.username,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src;
                    """,
                    {
                        'h_user_pk': h_user_pk,
                        'username': username,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )
                
    def mart_user_category_counters_select(self, 
                                           user_id: uuid.UUID,
                                           category_id: uuid.UUID
                                           ) -> Optional[UserCategoryCountersSetting]:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        select  lou.h_user_pk as user_id, 
                                hc.h_category_pk as category_id, 
                                hc.category_name,
                                count(lop.h_order_pk) order_cnt
                        from dds.h_category hc 
                        inner join dds.l_product_category lpc on lpc.h_category_pk = hc.h_category_pk 
                        inner join dds.l_order_product lop on lop.h_product_pk = lpc.h_product_pk 
                        inner join dds.l_order_user lou on lou.h_order_pk = lop.h_order_pk 
                        where lou.h_user_pk = %(user_id)s
                        and hc.h_category_pk = %(category_id)s
                        group by lou.h_user_pk, 
                                hc.h_category_pk, 
                                hc.category_name;
                    """,
                    {
                        "user_id": user_id,
                        "category_id": category_id 
                    }
                )
                return cur.fetchone()
                
    def mart_user_product_counters_select(self, 
                                           user_id: uuid.UUID,
                                           product_id: uuid.UUID) -> Optional[UserProductCountersSetting]:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        select  lou.h_user_pk as user_id, 
                                hp.h_product_pk as product_id, 
                                spn."name" as product_name,
                                count(lop.h_order_pk) order_cnt
                        from dds.h_product hp  
                        inner join dds.l_order_product lop on lop.h_product_pk = hp.h_product_pk 
                        inner join dds.l_order_user lou on lou.h_order_pk = lop.h_order_pk 
                        left join dds.s_product_names spn on spn.h_product_pk = hp.h_product_pk 
                        where lou.h_user_pk = %(user_id)s
                        and hp.h_product_pk = %(product_id)s
                        group by lou.h_user_pk, 
                                hp.h_product_pk, 
                                spn."name";
                    """,
                    {
                        "user_id": user_id,
                        "product_id": product_id 
                    }
                )
                return cur.fetchone()
