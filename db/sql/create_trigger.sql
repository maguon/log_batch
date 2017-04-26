delimiter $$
create trigger trg_create_storage_stat
after insert on storage_info for each row
BEGIN
insert into storage_stat_date(date_id,storage_id)
values(DATE_FORMAT(CURRENT_DATE(),'%Y%m%d'),new.id);
END $$
delimiter ;


delimiter $$
create trigger trg_increase_storage_stat
after insert on car_storage_rel for each row
BEGIN
update storage_stat_date set imports=imports+1,balance=balance+1
where date_id=DATE_FORMAT(CURRENT_DATE(),'%Y%m%d')and storage_id=new.storage_id;
END $$
delimiter ;


delimiter $$
DROP TRIGGER IF EXISTS trg_decrease_storage_stat;
create  trigger trg_decrease_storage_stat
after update on car_storage_rel for each row
BEGIN
IF (old.rel_status <>2 and new.rel_status=2)THEN
update storage_stat_date set exports=exports+1,balance=balance-1
where date_id=DATE_FORMAT(CURRENT_DATE(),'%Y%m%d')and storage_id=new.storage_id;
END IF;
END $$
delimiter ;